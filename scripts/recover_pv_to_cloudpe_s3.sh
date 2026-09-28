#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

usage() {
  cat <<'EOF'
Recover tickrecorder data from the Kubernetes PVC and upload available files to
CloudPE S3.

Usage:
  scripts/recover_pv_to_cloudpe_s3.sh [DATE]

DATE defaults to today's date in Asia/Kolkata, formatted as YYYYMMDD.

Common overrides:
  NAMESPACE=botspace
  PVC_NAME=tickrecorder-data
  APP_LABEL=tickrecorder
  JOB_NAME=tickrecorder
  CONTAINER_NAME=tickrecorder
  HELM_VALUES_FILE=/path/to/values.yaml
  CREDS_FILE=/path/to/cloudpe_s3_bucket.sh
  WORK_DIR=/tmp/tickrecorder-pv-recover-YYYYMMDD.xxxxxx
  STRICT=true
  DRY_RUN=false
  HELPER_IMAGE=alpine:3.20
  PVC_SUB_PATH=

The script first tries to stream data from a running tickrecorder pod. If none is
running, it creates a short-lived helper pod that mounts the PVC read-only,
streams the requested date as a tarball, and deletes the helper pod.

DRY_RUN=true still reads Kubernetes/PVC data and stages files locally, but does
not upload to S3. STRICT=false skips unfinished .inprogress files when rebuilding
an archive for partial recovery. Existing S3 objects at the destination keys are overwritten. Local staging files are kept.

Uploads DATE_trade_ticks.tar.gz to CLOUDPE_S3_PREFIX/DATE/DATE_trade_ticks.tar.gz.
The archive contains DATE/ (control, symbolupdate, tbtdepth). A recovered date
partition is always rebuilt so an older archive cannot hide newer parts. If only
an archive survives, it is validated and reused. Run after recording has stopped
to recover a complete date; an active pod can only provide a snapshot of disk data.
Run metadata under _runs and upload receipts under _uploads are not uploaded.

It does not require the tickrecorder repo or Helm repo to exist locally. S3 env
is loaded from, in order: current environment, CREDS_FILE, HELM_VALUES_FILE when
provided, then the live Kubernetes Job/Pod manifest. Kubernetes env values may
be direct values or valueFrom Secret/ConfigMap references, if RBAC allows reads.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if (($# > 1)); then
  printf 'ERROR: Expected at most one DATE argument\n' >&2
  exit 1
fi
command -v python3 >/dev/null 2>&1 || { printf 'ERROR: Missing python3\n' >&2; exit 1; }

DATE="${1:-${DATE:-$(TZ=Asia/Kolkata date +%Y%m%d)}}"
if ! python3 - "${DATE}" <<'PYDATE'
from datetime import datetime
import sys
value = sys.argv[1]
if len(value) != 8 or not value.isdigit():
    raise SystemExit("DATE must be YYYYMMDD")
try:
    datetime.strptime(value, "%Y%m%d")
except ValueError:
    raise SystemExit("DATE must be a valid calendar date")
PYDATE
then
  exit 1
fi

NAMESPACE="${NAMESPACE:-botspace}"
PVC_NAME="${PVC_NAME:-tickrecorder-data}"
APP_LABEL="${APP_LABEL:-tickrecorder}"
JOB_NAME="${JOB_NAME:-tickrecorder}"
CONTAINER_NAME="${CONTAINER_NAME:-tickrecorder}"
HELM_VALUES_FILE="${HELM_VALUES_FILE:-}"
CREDS_FILE="${CREDS_FILE:-}"
WORK_DIR="${WORK_DIR:-$(mktemp -d "/tmp/tickrecorder-pv-recover-${DATE}.XXXXXX")}"
DATA_DIR="${WORK_DIR}/data"
PV_ARCHIVE="${WORK_DIR}/pv-data-${DATE}.tgz"
PY_DEPS_DIR="${PY_DEPS_DIR:-/tmp/tickrecorder-pv-recover-python-deps}"
STRICT="${STRICT:-true}"
DRY_RUN="${DRY_RUN:-false}"

HELPER_POD="${HELPER_POD:-tickrecorder-cloudpe-recover-${DATE}-$$}"
HELPER_IMAGE="${HELPER_IMAGE:-alpine:3.20}"
PVC_SUB_PATH="${PVC_SUB_PATH:-}"
HELPER_REMOTE_DATA_DIR="/pvdata"
APP_REMOTE_DATA_DIR="${APP_REMOTE_DATA_DIR:-/app/data}"
SOURCE_POD=""
SOURCE_REMOTE_DATA_DIR=""
SOURCE_CONTAINER=""
CREATED_HELPER_POD=false

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

cleanup() {
  if [[ "${CREATED_HELPER_POD}" == "true" ]]; then
    log "Deleting helper pod ${HELPER_POD}"
    kubectl delete pod "${HELPER_POD}" -n "${NAMESPACE}" --ignore-not-found=true >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

load_creds_file() {
  if [[ -n "${CREDS_FILE}" && -f "${CREDS_FILE}" ]]; then
    log "Loading CloudPE S3 credentials from ${CREDS_FILE}"
    local name
    local -A overrides=()
    for name in CLOUDPE_S3_ENDPOINT_URL CLOUDPE_S3_REGION CLOUDPE_S3_BUCKET_NAME CLOUDPE_S3_PREFIX CLOUDPE_S3_ACCESS_KEY_ID CLOUDPE_S3_SECRET_ACCESS_KEY; do
      if [[ -n "${!name:-}" ]]; then
        overrides["${name}"]="${!name}"
      fi
    done
    set +u
    # shellcheck source=/dev/null
    . "${CREDS_FILE}"
    set -u
    for name in "${!overrides[@]}"; do
      export "${name}=${overrides[${name}]}"
    done
  elif [[ -n "${CREDS_FILE}" ]]; then
    die "Credentials file not found: ${CREDS_FILE}"
  fi
}

export_existing_s3_env() {
  local name
  for name in \
    CLOUDPE_S3_ENDPOINT_URL \
    CLOUDPE_S3_REGION \
    CLOUDPE_S3_BUCKET_NAME \
    CLOUDPE_S3_PREFIX \
    CLOUDPE_S3_ACCESS_KEY_ID \
    CLOUDPE_S3_SECRET_ACCESS_KEY; do
    if [[ -n "${!name:-}" ]]; then
      export "${name}"
    fi
  done
}

load_missing_s3_env_from_values() {
  [[ -n "${HELM_VALUES_FILE}" ]] || return 0
  [[ -f "${HELM_VALUES_FILE}" ]] || die "Helm values file not found: ${HELM_VALUES_FILE}"

  export_existing_s3_env

  local exports
  exports="$(HELM_VALUES_FILE="${HELM_VALUES_FILE}" python3 <<'PY'
import os
import shlex
from pathlib import Path

keys = {
    "CLOUDPE_S3_ENDPOINT_URL",
    "CLOUDPE_S3_REGION",
    "CLOUDPE_S3_BUCKET_NAME",
    "CLOUDPE_S3_PREFIX",
    "CLOUDPE_S3_ACCESS_KEY_ID",
    "CLOUDPE_S3_SECRET_ACCESS_KEY",
}

values_path = Path(os.environ["HELM_VALUES_FILE"])
env_values = {}
in_env = False
env_indent = None

for raw_line in values_path.read_text(encoding="utf-8").splitlines():
    stripped = raw_line.strip()
    if not stripped or stripped.startswith("#"):
        continue

    indent = len(raw_line) - len(raw_line.lstrip(" "))
    if stripped == "env:":
        in_env = True
        env_indent = indent
        continue

    if in_env and indent <= env_indent:
        break

    if not in_env or ":" not in stripped:
        continue

    key, value = stripped.split(":", 1)
    key = key.strip()
    if key not in keys:
        continue

    value = value.strip()
    if (
        (value.startswith('"') and value.endswith('"'))
        or (value.startswith("'") and value.endswith("'"))
    ):
        value = value[1:-1]

    env_values[key] = value

for key in sorted(keys):
    if os.environ.get(key):
        continue
    value = env_values.get(key)
    if value:
        print(f"export {key}={shlex.quote(value)}")
PY
)"

  if [[ -n "${exports}" ]]; then
    log "Loading missing CloudPE S3 settings from ${HELM_VALUES_FILE}"
    eval "${exports}"
  fi
}

load_missing_s3_env_from_k8s() {
  export_existing_s3_env

  local workload_json=""
  if workload_json="$(kubectl get job "${JOB_NAME}" -n "${NAMESPACE}" -o json 2>/dev/null)"; then
    :
  elif workload_json="$(kubectl get pods -n "${NAMESPACE}" -l "app=${APP_LABEL}" -o json 2>/dev/null)"; then
    :
  else
    return 0
  fi

  local exports
  exports="$(WORKLOAD_JSON="${workload_json}" CONTAINER_NAME="${CONTAINER_NAME}" NAMESPACE="${NAMESPACE}" python3 <<'PY'
import base64
import json
import os
import subprocess
import shlex
import sys

keys = {
    "CLOUDPE_S3_ENDPOINT_URL",
    "CLOUDPE_S3_REGION",
    "CLOUDPE_S3_BUCKET_NAME",
    "CLOUDPE_S3_PREFIX",
    "CLOUDPE_S3_ACCESS_KEY_ID",
    "CLOUDPE_S3_SECRET_ACCESS_KEY",
}

obj = json.loads(os.environ["WORKLOAD_JSON"])
container_name = os.environ["CONTAINER_NAME"]
namespace = os.environ["NAMESPACE"]


def pod_specs(workload):
    kind = workload.get("kind")
    if kind == "Job":
        spec = workload.get("spec", {}).get("template", {}).get("spec", {})
        if spec:
            yield spec
    elif kind == "Pod":
        spec = workload.get("spec", {})
        if spec:
            yield spec
    else:
        for item in sorted(workload.get("items", []), key=lambda p: p.get("metadata", {}).get("creationTimestamp", ""), reverse=True):
            spec = item.get("spec", {})
            if spec:
                yield spec


def kubectl_json(kind, name):
    cmd = ["kubectl", "get", kind, name, "-n", namespace, "-o", "json"]
    try:
        raw = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return None
    return json.loads(raw)


def secret_value(secret_name, key):
    secret = kubectl_json("secret", secret_name)
    if not secret:
        return None

    encoded = secret.get("data", {}).get(key)
    if encoded is None:
        value = secret.get("stringData", {}).get(key)
        return value

    try:
        return base64.b64decode(encoded).decode("utf-8")
    except Exception:
        print(
            f"warning: could not decode secret {secret_name}/{key}",
            file=sys.stderr,
        )
        return None


def config_map_value(config_map_name, key):
    config_map = kubectl_json("configmap", config_map_name)
    if not config_map:
        return None
    return config_map.get("data", {}).get(key)


def apply_env_from(container, values):
    for source in container.get("envFrom", []):
        if "secretRef" in source:
            secret_name = source["secretRef"].get("name")
            if not secret_name:
                continue
            secret = kubectl_json("secret", secret_name)
            if not secret:
                continue
            for name, encoded in secret.get("data", {}).items():
                name = source.get("prefix", "") + name
                if name in keys:
                    try:
                        values[name] = base64.b64decode(encoded).decode("utf-8")
                    except Exception:
                        print(
                            f"warning: could not decode secret {secret_name}/{name}",
                            file=sys.stderr,
                        )
        elif "configMapRef" in source:
            config_map_name = source["configMapRef"].get("name")
            if not config_map_name:
                continue
            config_map = kubectl_json("configmap", config_map_name)
            if not config_map:
                continue
            for name, value in config_map.get("data", {}).items():
                name = source.get("prefix", "") + name
                if name in keys:
                    values[name] = value


def env_value(env):
    if "value" in env:
        return env["value"]

    value_from = env.get("valueFrom", {})
    if "secretKeyRef" in value_from:
        ref = value_from["secretKeyRef"]
        secret_name = ref.get("name")
        key = ref.get("key")
        if secret_name and key:
            return secret_value(secret_name, key)

    if "configMapKeyRef" in value_from:
        ref = value_from["configMapKeyRef"]
        config_map_name = ref.get("name")
        key = ref.get("key")
        if config_map_name and key:
            return config_map_value(config_map_name, key)

    return None


values = {}
for spec in pod_specs(obj):
    containers = spec.get("containers", [])
    preferred = [c for c in containers if c.get("name") == container_name]
    for container in preferred or containers:
        apply_env_from(container, values)
        for env in container.get("env", []):
            name = env.get("name")
            if name in keys:
                value = env_value(env)
                if value is not None:
                    values[name] = value
        if values:
            break
    if values:
        break

for key in sorted(keys):
    if os.environ.get(key):
        continue
    value = values.get(key)
    if value:
        print(f"export {key}={shlex.quote(value)}")
PY
)"

  if [[ -n "${exports}" ]]; then
    log "Loading missing CloudPE S3 settings from Kubernetes workload"
    eval "${exports}"
  fi
}

check_s3_config() {
  export_existing_s3_env
  local name
  local missing=()
  for name in CLOUDPE_S3_ENDPOINT_URL CLOUDPE_S3_REGION CLOUDPE_S3_ACCESS_KEY_ID CLOUDPE_S3_SECRET_ACCESS_KEY; do
    if [[ -z "${!name:-}" ]]; then
      missing+=("${name}")
    fi
  done
  if ((${#missing[@]} > 0)); then
    die "Missing S3 configuration: ${missing[*]}. Set env vars, CREDS_FILE, or HELM_VALUES_FILE."
  fi
  export CLOUDPE_S3_BUCKET_NAME="${CLOUDPE_S3_BUCKET_NAME:-index-bucket}"
  export CLOUDPE_S3_PREFIX="${CLOUDPE_S3_PREFIX-index-bucket-holder/contracts}"
  export CLOUDPE_S3_REGION="${CLOUDPE_S3_REGION:-}"
}

find_running_app_pod() {
  kubectl get pods \
    -n "${NAMESPACE}" \
    -l "app=${APP_LABEL}" \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
    | head -n 1
}

create_helper_pod() {
  log "Creating helper pod ${HELPER_POD} to mount PVC ${PVC_NAME}"
  # Create only: never replace or delete a pre-existing pod with the same name.
  HELPER_POD="${HELPER_POD}" HELPER_IMAGE="${HELPER_IMAGE}" PVC_NAME="${PVC_NAME}" \
    PVC_SUB_PATH="${PVC_SUB_PATH}" python3 <<'PYMANIFEST' | kubectl create -n "${NAMESPACE}" -f -
import json
import os
mount = {"name": "data", "mountPath": "/pvdata", "readOnly": True}
if os.environ["PVC_SUB_PATH"]:
    mount["subPath"] = os.environ["PVC_SUB_PATH"]
print(json.dumps({
    "apiVersion": "v1", "kind": "Pod",
    "metadata": {"name": os.environ["HELPER_POD"], "labels": {"app": "tickrecorder-cloudpe-recover"}},
    "spec": {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "containers": [{"name": "recover", "image": os.environ["HELPER_IMAGE"],
                        "command": ["sh", "-c", "sleep 3600"], "volumeMounts": [mount]}],
        "volumes": [{"name": "data", "persistentVolumeClaim": {
            "claimName": os.environ["PVC_NAME"], "readOnly": True}}],
    },
}))
PYMANIFEST

  CREATED_HELPER_POD=true
  kubectl wait pod "${HELPER_POD}" \
    -n "${NAMESPACE}" \
    --for=condition=Ready \
    --timeout=180s >/dev/null
}

select_source_pod() {
  SOURCE_POD="$(find_running_app_pod)" || die "Unable to list running app pods"
  if [[ -n "${SOURCE_POD}" ]]; then
    SOURCE_REMOTE_DATA_DIR="${APP_REMOTE_DATA_DIR}"
    SOURCE_CONTAINER="${CONTAINER_NAME}"
    log "Using running tickrecorder pod ${SOURCE_POD}:${SOURCE_REMOTE_DATA_DIR}"
    return 0
  fi

  create_helper_pod
  SOURCE_POD="${HELPER_POD}"
  SOURCE_CONTAINER="recover"
  SOURCE_REMOTE_DATA_DIR="${HELPER_REMOTE_DATA_DIR}"
  log "Using helper pod ${SOURCE_POD}:${SOURCE_REMOTE_DATA_DIR}"
}

stream_pv_data() {
  kubectl get pvc "${PVC_NAME}" -n "${NAMESPACE}" >/dev/null

  select_source_pod

  log "Streaming date ${DATE} to ${PV_ARCHIVE}"
  kubectl exec -n "${NAMESPACE}" "${SOURCE_POD}" -c "${SOURCE_CONTAINER}" -- sh -c '
    cd "$1" || exit 1
    if [ -d "$2" ]; then
      tar -czf - "$2"
    elif [ -f "${2}_trade_ticks.tar.gz" ]; then
      tar -czf - "${2}_trade_ticks.tar.gz"
    else
      echo "No date directory or trade-ticks archive found for $2" >&2
      exit 1
    fi
  ' sh "${SOURCE_REMOTE_DATA_DIR}" "${DATE}" > "${PV_ARCHIVE}.partial"
  mv "${PV_ARCHIVE}.partial" "${PV_ARCHIVE}"

  # Refuse links, special files, and paths outside the requested date.
  PV_ARCHIVE="${PV_ARCHIVE}" DATA_DIR="${DATA_DIR}" DATE="${DATE}" python3 <<'PYEXTRACT'
import os
import tarfile
from pathlib import Path, PurePosixPath

root = Path(os.environ["DATA_DIR"])
root.mkdir()  # Never merge a recovery with stale staging files.
date = os.environ["DATE"]
with tarfile.open(os.environ["PV_ARCHIVE"], "r:gz") as archive:
    for member in archive.getmembers():
        path = PurePosixPath(member.name)
        if (path.is_absolute() or ".." in path.parts or not path.parts
                or path.parts[0] not in {date, f"{date}_trade_ticks.tar.gz"}
                or not (member.isfile() or member.isdir())):
            raise RuntimeError(f"Unsafe PVC archive member: {member.name}")
    archive.extractall(root)
PYEXTRACT
  log "Recovered PVC archive size: $(du -h "${PV_ARCHIVE}" | awk '{print $1}')"
}

prepare_recovered_files() {
  DATA_DIR="${DATA_DIR}" DATE="${DATE}" STRICT="${STRICT}" python3 <<'PYPREPARE'
import os
import tarfile
from pathlib import Path, PurePosixPath

root = Path(os.environ["DATA_DIR"])
date = os.environ["DATE"]
source = root / date
output = root / f"{date}_trade_ticks.tar.gz"
strict = os.environ["STRICT"] == "true"
if source.is_dir():
    paths = sorted(source.rglob("*"))
    unfinished = [path for path in paths if path.name.endswith(".inprogress")]
    if strict and unfinished:
        raise RuntimeError(f"Unfinished files found; use STRICT=false for partial recovery: {unfinished}")
    paths = [path for path in paths if not path.name.endswith(".inprogress")]
    if not any(path.is_file() and path.stat().st_size > 0 for path in paths):
        raise RuntimeError(f"No finalized data files found for {date}")
    if unfinished:
        print(f"Partial recovery: skipping {len(unfinished)} unfinished files", flush=True)
    temporary = output.with_suffix(".gz.partial")
    with tarfile.open(temporary, "w:gz") as archive:
        archive.add(source, arcname=date, recursive=False)
        for path in paths:
            archive.add(path, arcname=path.relative_to(root), recursive=False)
    temporary.replace(output)

if not output.is_file() or output.stat().st_size == 0:
    raise FileNotFoundError(f"No recovered trade-ticks archive for {date}")
with tarfile.open(output, "r:gz") as archive:
    file_count = 0
    for member in archive:
        path = PurePosixPath(member.name)
        if (path.is_absolute() or ".." in path.parts or not path.parts
                or path.parts[0] != date
                or not (member.isfile() or member.isdir())
                or path.name.endswith(".inprogress")):
            raise RuntimeError(f"Invalid trade-ticks archive member: {member.name}")
        if member.isfile() and member.size > 0:
            file_count += 1
    if not file_count:
        raise RuntimeError(f"Trade-ticks archive contains no data: {output}")
print(f"Prepared {output} ({file_count} files)", flush=True)
PYPREPARE
}

ensure_boto3() {
  if [[ -d "${PY_DEPS_DIR}/boto3" ]]; then
    export PYTHONPATH="${PY_DEPS_DIR}${PYTHONPATH:+:${PYTHONPATH}}"
    return 0
  fi

  if python3 -c 'import boto3' >/dev/null 2>&1; then
    return 0
  fi

  log "Installing boto3 into ${PY_DEPS_DIR}"
  mkdir -p "${PY_DEPS_DIR}"
  python3 -m pip install --quiet --target "${PY_DEPS_DIR}" 'boto3>=1.28.0'
  export PYTHONPATH="${PY_DEPS_DIR}${PYTHONPATH:+:${PYTHONPATH}}"
}

upload_recovered_files() {
  export RECOVERED_DATA_DIR="${DATA_DIR}"
  export TARGET_CLOUDPE_S3_BUCKET_NAME="${CLOUDPE_S3_BUCKET_NAME}"
  export TARGET_CLOUDPE_S3_KEY_PREFIX="${CLOUDPE_S3_PREFIX}"
  export DATE
  export STRICT
  export DRY_RUN

  python3 <<'PY'
import os
import sys
import hashlib
from pathlib import Path
from urllib.parse import urlparse, urlunparse


date = os.environ["DATE"]
data_dir = Path(os.environ["RECOVERED_DATA_DIR"])
bucket = os.environ["TARGET_CLOUDPE_S3_BUCKET_NAME"]
key_prefix = os.environ["TARGET_CLOUDPE_S3_KEY_PREFIX"].strip("/")
region = os.environ.get("CLOUDPE_S3_REGION", "").strip() or None
endpoint_url = os.environ["CLOUDPE_S3_ENDPOINT_URL"].strip()
access_key = os.environ["CLOUDPE_S3_ACCESS_KEY_ID"].strip()
secret_key = os.environ["CLOUDPE_S3_SECRET_ACCESS_KEY"].strip()
dry_run = os.environ.get("DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}


def upload_key(*parts: str) -> str:
    key = "/".join(str(part).strip("/") for part in parts if part)
    bucket_prefix = f"{bucket}/"
    if key.startswith(bucket_prefix):
        key = key[len(bucket_prefix):]
    return key


if "://" not in endpoint_url:
    endpoint_url = f"https://{endpoint_url}"
parsed_endpoint = urlparse(endpoint_url)
expected_host = f"s3.{region}.purestore.io" if region else ""
if expected_host and (parsed_endpoint.netloc == expected_host
                      or parsed_endpoint.netloc.endswith(f".{expected_host}")):
    endpoint_url = urlunparse(parsed_endpoint._replace(
        netloc=expected_host, path="", params="", query="", fragment=""))
if parsed_endpoint.scheme not in {"http", "https"} or not parsed_endpoint.netloc:
    raise ValueError("CLOUDPE_S3_ENDPOINT_URL must be an http(s) URL")

local_path = data_dir / f"{date}_trade_ticks.tar.gz"
key = upload_key(key_prefix, date, local_path.name)
uploads = [(local_path, key)]

print(f"Uploading recovered tickrecorder files to s3://{bucket}/ via {endpoint_url}")
for local_path, key in uploads:
    size = local_path.stat().st_size
    print(f"upload {local_path.name} ({size} bytes) -> s3://{bucket}/{key}", flush=True)

if dry_run:
    print("DRY_RUN=true; no objects uploaded.")
    sys.exit(0)

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

s3 = boto3.client(
    "s3",
    region_name=region,
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                  connect_timeout=10, read_timeout=60,
                  retries={"max_attempts": 5, "mode": "standard"}),
)

# Match the taperecorder recovery script: avoid UploadPart for archives below
# 5 GiB because CloudPE may reject multipart requests for these credentials.
transfer_config = TransferConfig(multipart_threshold=5 * 1024 * 1024 * 1024)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


for local_path, key in uploads:
    expected_size = local_path.stat().st_size
    expected_digest = sha256(local_path)
    s3.upload_file(str(local_path), bucket, key, ExtraArgs={
        "ContentType": "application/gzip", "Metadata": {"sha256": expected_digest},
    }, Config=transfer_config)
    head = s3.head_object(Bucket=bucket, Key=key)
    if head["ContentLength"] != expected_size:
        raise RuntimeError(f"Uploaded size mismatch for s3://{bucket}/{key}")
    if head.get("Metadata", {}).get("sha256") != expected_digest:
        raise RuntimeError(f"Uploaded checksum metadata mismatch for s3://{bucket}/{key}")
    print(f"Reading back s3://{bucket}/{key} for SHA-256 verification", flush=True)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    digest = hashlib.sha256()
    remote_size = 0
    try:
        for block in iter(lambda: body.read(1024 * 1024), b""):
            digest.update(block)
            remote_size += len(block)
    finally:
        body.close()
    if remote_size != expected_size or digest.hexdigest() != expected_digest:
        raise RuntimeError(f"Read-back checksum/size mismatch for s3://{bucket}/{key}")
    print(f"ok s3://{bucket}/{key} ({remote_size} bytes, sha256={expected_digest})")
PY
}

main() {
  require_cmd kubectl
  require_cmd tar
  require_cmd python3
  require_cmd find

  [[ "${DRY_RUN}" == "true" || "${DRY_RUN}" == "false" ]] || die "DRY_RUN must be true or false"
  [[ "${STRICT}" == "true" || "${STRICT}" == "false" ]] || die "STRICT must be true or false"
  mkdir -p "${WORK_DIR}"
  [[ ! -e "${DATA_DIR}" ]] || die "Use a fresh WORK_DIR; ${DATA_DIR} already exists"
  load_creds_file
  load_missing_s3_env_from_values
  load_missing_s3_env_from_k8s
  check_s3_config
  stream_pv_data
  prepare_recovered_files
  if [[ "${DRY_RUN}" != "true" ]]; then
    ensure_boto3
  fi
  upload_recovered_files

  log "Recovery finished (DRY_RUN=${DRY_RUN})"
  log "Staging directory kept at: ${WORK_DIR}"
}

main "$@"
