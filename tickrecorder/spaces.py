from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tarfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse, urlunparse

import boto3
from botocore.config import Config

from tickrecorder.logger import create_logger


LOG = create_logger(__name__)
TRADE_DATE_PATTERN = re.compile(r"^[0-9]{8}$")
DEFAULT_BUCKET_NAME = "index-bucket"
DEFAULT_SPACES_PREFIX = "index-bucket-holder/contracts"


@dataclass(frozen=True)
class DigitalOceanSpacesConfig:
    endpoint_url: str
    region: str
    bucket_name: str
    prefix: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)


@dataclass(frozen=True)
class ArchiveArtifact:
    trading_date: str
    source_dir: Path
    archive_path: Path
    file_count: int
    size_bytes: int
    sha256: str
    source_fingerprint: str


@dataclass(frozen=True)
class UploadReceipt:
    bucket_name: str
    object_key: str
    size_bytes: int
    sha256: str
    etag: str | None

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket_name}/{self.object_key}"


def normalize_s3_key(bucket_name: str, key: str) -> str:
    """Remove leading separators and an accidentally duplicated bucket name."""

    normalized_key = key.lstrip("/")
    bucket_prefix = f"{bucket_name}/"
    if bucket_name and normalized_key.startswith(bucket_prefix):
        normalized_key = normalized_key[len(bucket_prefix) :]
    return normalized_key


def normalize_do_spaces_endpoint_url(
    endpoint_url: str,
    region: str,
    bucket_name: str,
) -> str:
    """Return a regional endpoint suitable for boto3's Bucket argument."""

    endpoint_url = endpoint_url.strip()
    if not endpoint_url:
        return endpoint_url

    endpoint_with_scheme = (
        endpoint_url if "://" in endpoint_url else f"https://{endpoint_url}"
    )
    parsed = urlparse(endpoint_with_scheme)
    expected_host = f"{region}.digitaloceanspaces.com" if region else ""
    bucket_host_suffix = f".{expected_host}" if expected_host else ""
    is_regional_endpoint = bool(expected_host and parsed.netloc == expected_host)
    is_bucket_endpoint = bool(
        parsed.netloc
        and bucket_host_suffix
        and parsed.netloc.endswith(bucket_host_suffix)
        and parsed.netloc[: -len(bucket_host_suffix)]
    )
    if is_regional_endpoint or is_bucket_endpoint:
        return urlunparse(
            parsed._replace(
                netloc=expected_host,
                path="",
                params="",
                query="",
                fragment="",
            )
        )
    return endpoint_with_scheme


def finalized_trading_dates(parts: list[dict[str, Any]]) -> tuple[str, ...]:
    """Extract the date partitions written by the current recorder run."""

    trading_dates: set[str] = set()
    for part in parts:
        relative_path = PurePosixPath(str(part.get("path", "")))
        if not relative_path.parts:
            raise RuntimeError("Writer reported a part without a relative path")
        trading_date = relative_path.parts[0]
        if not TRADE_DATE_PATTERN.fullmatch(trading_date):
            raise RuntimeError(
                f"Writer reported an invalid trading-date partition: {trading_date!r}"
            )
        trading_dates.add(trading_date)
    if not trading_dates:
        raise RuntimeError("Recorder produced no finalized date partitions to upload")
    return tuple(sorted(trading_dates))


def pending_trading_dates(
    data_dir: Path,
    bucket_name: str = DEFAULT_BUCKET_NAME,
    prefix: str = DEFAULT_SPACES_PREFIX,
) -> tuple[str, ...]:
    """Return retained date directories without a valid verified-upload receipt."""

    trading_dates: list[str] = []
    if not data_dir.is_dir():
        return ()
    for path in data_dir.iterdir():
        if (
            path.is_dir()
            and not path.is_symlink()
            and TRADE_DATE_PATTERN.fullmatch(path.name)
            and not _has_verified_upload_receipt(
                data_dir,
                path.name,
                path,
                bucket_name,
                trade_ticks_object_key(bucket_name, prefix, path.name),
            )
        ):
            trading_dates.append(path.name)
    return tuple(sorted(trading_dates))


def write_upload_receipt_atomic(
    data_dir: Path,
    trading_date: str,
    payload: dict[str, Any],
) -> Path:
    """Persist proof of a verified upload so failed prior dates can be retried."""

    if not TRADE_DATE_PATTERN.fullmatch(trading_date):
        raise ValueError(f"Invalid trading date: {trading_date!r}")
    receipt_dir = data_dir / "_uploads"
    receipt_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = receipt_dir / f"{trading_date}.json"
    temporary_path = receipt_dir / (
        f".{receipt_path.name}.{uuid.uuid4().hex}.inprogress"
    )
    receipt_payload = {
        **payload,
        "trading_date": trading_date,
        "upload_status": "verified",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        with temporary_path.open("x", encoding="utf-8") as handle:
            json.dump(
                receipt_payload,
                handle,
                indent=2,
                sort_keys=True,
                default=str,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, receipt_path)
        _fsync_directory(receipt_dir)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
    LOG.info("Wrote verified-upload receipt path=%s", receipt_path)
    return receipt_path


def create_trade_ticks_archive(data_dir: Path, trading_date: str) -> ArchiveArtifact:
    """Atomically archive one finalized YYYYMMDD directory as a .tar.gz file."""

    if not TRADE_DATE_PATTERN.fullmatch(trading_date):
        raise ValueError(f"Invalid trading date: {trading_date!r}")

    source_dir = data_dir / trading_date
    if source_dir.is_symlink() or not source_dir.is_dir():
        raise FileNotFoundError(f"Trading-date directory not found: {source_dir}")

    source_paths, file_count, source_fingerprint = _snapshot_source_tree(
        source_dir
    )
    if file_count == 0:
        raise RuntimeError(f"Trading-date directory contains no files: {source_dir}")

    archive_path = data_dir / f"{trading_date}_trade_ticks.tar.gz"
    temporary_path = data_dir / (
        f".{archive_path.name}.{uuid.uuid4().hex}.inprogress"
    )
    try:
        with tarfile.open(temporary_path, "w:gz") as archive:
            archive.add(source_dir, arcname=trading_date, recursive=False)
            for path in source_paths:
                relative_path = path.relative_to(source_dir)
                archive.add(
                    path,
                    arcname=str(Path(trading_date) / relative_path),
                    recursive=False,
                )
        _, final_file_count, final_source_fingerprint = _snapshot_source_tree(
            source_dir
        )
        if (
            final_file_count != file_count
            or final_source_fingerprint != source_fingerprint
        ):
            raise RuntimeError(
                f"Trading-date directory changed while being archived: {source_dir}"
            )
        _fsync_file(temporary_path)
        os.replace(temporary_path, archive_path)
        _fsync_directory(data_dir)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise

    size_bytes = archive_path.stat().st_size
    digest = _sha256(archive_path)
    LOG.info(
        "Created trade-ticks archive date=%s files=%d bytes=%d path=%s sha256=%s",
        trading_date,
        file_count,
        size_bytes,
        archive_path,
        digest,
    )
    return ArchiveArtifact(
        trading_date=trading_date,
        source_dir=source_dir,
        archive_path=archive_path,
        file_count=file_count,
        size_bytes=size_bytes,
        sha256=digest,
        source_fingerprint=source_fingerprint,
    )


def trade_ticks_object_key(
    bucket_name: str,
    prefix: str,
    trading_date: str,
) -> str:
    """Build the exact object key for one date's trade-tick archive."""

    if not TRADE_DATE_PATTERN.fullmatch(trading_date):
        raise ValueError(f"Invalid trading date: {trading_date!r}")
    return normalize_s3_key(
        bucket_name,
        "/".join(
            part.strip("/")
            for part in (
                prefix,
                trading_date,
                f"{trading_date}_trade_ticks.tar.gz",
            )
            if part.strip("/")
        ),
    )


def upload_trade_ticks_archive(
    artifact: ArchiveArtifact,
    spaces: DigitalOceanSpacesConfig,
    *,
    client: Any | None = None,
) -> UploadReceipt:
    """Upload an archive, then read it back to verify size and SHA-256."""

    if not artifact.archive_path.is_file():
        raise FileNotFoundError(f"Archive not found: {artifact.archive_path}")

    object_key = trade_ticks_object_key(
        spaces.bucket_name,
        spaces.prefix,
        artifact.trading_date,
    )
    if client is None:
        client_kwargs: dict[str, Any] = {
            "region_name": spaces.region,
            "endpoint_url": spaces.endpoint_url,
            "aws_access_key_id": spaces.access_key_id,
            "aws_secret_access_key": spaces.secret_access_key,
        }
        if "digitaloceanspaces.com" in spaces.endpoint_url:
            client_kwargs["config"] = Config(
                connect_timeout=10,
                read_timeout=60,
                retries={"max_attempts": 5, "mode": "standard"},
                s3={"addressing_style": "virtual"},
            )
        client = boto3.client("s3", **client_kwargs)

    LOG.info(
        "Uploading trade-ticks archive bytes=%d destination=s3://%s/%s endpoint=%s",
        artifact.size_bytes,
        spaces.bucket_name,
        object_key,
        spaces.endpoint_url,
    )
    client.upload_file(
        str(artifact.archive_path),
        spaces.bucket_name,
        object_key,
        ExtraArgs={
            "ContentType": "application/gzip",
            "Metadata": {"sha256": artifact.sha256},
        },
    )
    remote_metadata = client.head_object(
        Bucket=spaces.bucket_name,
        Key=object_key,
    )
    remote_size = int(remote_metadata.get("ContentLength", -1))
    if remote_size != artifact.size_bytes:
        raise RuntimeError(
            "DigitalOcean Spaces upload size mismatch "
            f"local={artifact.size_bytes} remote={remote_size}"
        )

    metadata_sha256 = str(
        remote_metadata.get("Metadata", {}).get("sha256", "")
    )
    if metadata_sha256 != artifact.sha256:
        raise RuntimeError(
            "DigitalOcean Spaces upload checksum metadata mismatch "
            f"local={artifact.sha256} remote={metadata_sha256 or '<missing>'}"
        )

    LOG.info(
        "Reading back trade-ticks archive for checksum verification "
        "destination=s3://%s/%s",
        spaces.bucket_name,
        object_key,
    )
    remote_object = client.get_object(
        Bucket=spaces.bucket_name,
        Key=object_key,
    )
    remote_body = remote_object.get("Body")
    if remote_body is None:
        raise RuntimeError("DigitalOcean Spaces read-back response has no body")
    remote_digest = hashlib.sha256()
    remote_bytes = 0
    try:
        for block in iter(lambda: remote_body.read(1024 * 1024), b""):
            remote_digest.update(block)
            remote_bytes += len(block)
    finally:
        close_body = getattr(remote_body, "close", None)
        if callable(close_body):
            close_body()
    if remote_bytes != artifact.size_bytes:
        raise RuntimeError(
            "DigitalOcean Spaces read-back size mismatch "
            f"local={artifact.size_bytes} remote={remote_bytes}"
        )
    remote_sha256 = remote_digest.hexdigest()
    if remote_sha256 != artifact.sha256:
        raise RuntimeError(
            "DigitalOcean Spaces read-back checksum mismatch "
            f"local={artifact.sha256} remote={remote_sha256}"
        )

    raw_etag = remote_metadata.get("ETag")
    etag = str(raw_etag).strip('"') if raw_etag is not None else None
    receipt = UploadReceipt(
        bucket_name=spaces.bucket_name,
        object_key=object_key,
        size_bytes=remote_size,
        sha256=remote_sha256,
        etag=etag,
    )
    LOG.info(
        "Uploaded and verified trade-ticks archive destination=%s bytes=%d sha256=%s",
        receipt.uri,
        receipt.size_bytes,
        receipt.sha256,
    )
    return receipt


def _has_verified_upload_receipt(
    data_dir: Path,
    trading_date: str,
    source_dir: Path,
    expected_bucket_name: str,
    expected_object_key: str,
) -> bool:
    receipt_path = data_dir / "_uploads" / f"{trading_date}.json"
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return False
    receipt_is_valid = (
        payload.get("trading_date") == trading_date
        and payload.get("upload_status") == "verified"
        and bool(payload.get("sha256"))
        and payload.get("bucket_name") == expected_bucket_name
        and payload.get("object_key") == expected_object_key
        and bool(payload.get("source_fingerprint"))
    )
    if not receipt_is_valid:
        return False
    try:
        _, _, current_fingerprint = _snapshot_source_tree(source_dir)
    except (OSError, RuntimeError):
        return False
    return payload.get("source_fingerprint") == current_fingerprint


def _snapshot_source_tree(source_dir: Path) -> tuple[list[Path], int, str]:
    """Return a stable inventory digest for an immutable date directory."""

    source_paths = sorted(
        source_dir.rglob("*"),
        key=lambda path: path.relative_to(source_dir).as_posix(),
    )
    digest = hashlib.sha256()
    file_count = 0
    for path in source_paths:
        relative_path = path.relative_to(source_dir).as_posix()
        path_stat = path.lstat()
        if stat.S_ISLNK(path_stat.st_mode):
            raise RuntimeError(f"Refusing to archive symbolic link: {path}")
        if path.name.endswith(".inprogress"):
            raise RuntimeError(f"Refusing to archive unfinished file: {path}")
        if stat.S_ISREG(path_stat.st_mode):
            path_type = "file"
            file_count += 1
        elif stat.S_ISDIR(path_stat.st_mode):
            path_type = "directory"
        else:
            raise RuntimeError(f"Refusing to archive special file: {path}")
        digest.update(
            (
                f"{path_type}\0{relative_path}\0{path_stat.st_size}\0"
                f"{path_stat.st_mtime_ns}\n"
            ).encode("utf-8")
        )
    return source_paths, file_count, digest.hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
