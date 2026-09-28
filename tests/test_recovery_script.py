"""Exercise the standalone recovery command without Kubernetes or S3 access."""
import json
import os
from pathlib import Path
import subprocess
import tarfile

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/recover_pv_to_cloudpe_s3.sh'
DATE = '20260928'


@pytest.fixture
def recovery(tmp_path):
    remote = tmp_path / 'remote'
    part = remote / DATE / 'symbolupdate' / 'part.parquet'
    part.parent.mkdir(parents=True)
    part.write_bytes(b'finalized data')
    bins = tmp_path / 'bin'
    bins.mkdir()
    kubectl = bins / 'kubectl'
    kubectl.write_text('''#!/usr/bin/env python3
import json, os, subprocess, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ['CALLS'], 'a') as out:
    out.write(json.dumps(args) + '\\n')
if args[:2] == ['get', 'job']:
    print(json.dumps({'kind': 'Job', 'spec': {'template': {'spec': {'containers': [
        {'name': 'tickrecorder', 'env': [
            {'name': 'CLOUDPE_S3_ENDPOINT_URL', 'value': 'https://s3.in-west2.purestore.io'},
            {'name': 'CLOUDPE_S3_REGION', 'value': 'in-west2'},
            {'name': 'CLOUDPE_S3_ACCESS_KEY_ID', 'value': 'test-key'},
            {'name': 'CLOUDPE_S3_SECRET_ACCESS_KEY', 'value': 'test-secret'}
        ]}]}}}}))
elif args[:2] == ['get', 'pods']:
    if os.environ.get('HELPER') != 'true':
        print('tickrecorder-running')
elif args[0] == 'create':
    Path(os.environ['MANIFEST']).write_text(sys.stdin.read())
elif args[0] == 'exec':
    command = args[args.index('--') + 1:]
    command = [os.environ['REMOTE'] if x in ('/app/data', '/pvdata') else x for x in command]
    sys.exit(subprocess.call(command))
''')
    kubectl.chmod(0o755)
    deps = tmp_path / 'deps'
    (deps / 'botocore').mkdir(parents=True)
    (deps / 'botocore/__init__.py').write_text('')
    (deps / 'botocore/config.py').write_text('class Config:\n    def __init__(self, **kw): pass\n')
    (deps / 'boto3/s3').mkdir(parents=True)
    (deps / 'boto3/s3/__init__.py').write_text('')
    (deps / 'boto3/s3/transfer.py').write_text(
        'class TransferConfig:\n'
        '    def __init__(self, multipart_threshold=8 * 1024 * 1024):\n'
        '        self.multipart_threshold = multipart_threshold\n'
    )
    (deps / 'boto3/__init__.py').write_text('''import io, json, os
from pathlib import Path
class Client:
    def upload_file(self, path, bucket, key, ExtraArgs, Config=None):
        threshold = Config.multipart_threshold if Config else 8 * 1024 * 1024
        if Path(path).stat().st_size >= threshold:
            raise RuntimeError('AccessDenied when calling UploadPart')
        self.data = Path(path).read_bytes()
        self.metadata = ExtraArgs['Metadata']
        Path(os.environ['UPLOAD']).write_text(json.dumps({'bucket': bucket, 'key': key}))
    def head_object(self, **kw):
        return {'ContentLength': len(self.data), 'Metadata': self.metadata}
    def get_object(self, **kw):
        data = self.data
        if os.environ.get('CORRUPT') == 'true':
            data = bytes([data[0] ^ 1]) + data[1:]
        return {'Body': io.BytesIO(data)}
def client(*args, **kw): return Client()
''')
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('CLOUDPE_') and key not in {
               'CREDS_FILE', 'HELM_VALUES_FILE', 'WORK_DIR', 'STRICT', 'DRY_RUN',
               'NAMESPACE', 'PVC_NAME', 'APP_LABEL', 'JOB_NAME', 'CONTAINER_NAME',
               'APP_REMOTE_DATA_DIR', 'HELPER_POD', 'PVC_SUB_PATH',
           }}
    env.update(PATH=f'{bins}:{env["PATH"]}', REMOTE=str(remote),
               CALLS=str(tmp_path / 'calls'), MANIFEST=str(tmp_path / 'manifest'),
               UPLOAD=str(tmp_path / 'upload'), PYTHONPATH=str(deps),
               PY_DEPS_DIR=str(tmp_path / 'unused-deps'),
               WORK_DIR=str(tmp_path / 'work'), DRY_RUN='true', STRICT='true')

    def run(date=DATE, **overrides):
        return subprocess.run(['bash', str(SCRIPT), date], env={**env, **overrides},
                              capture_output=True, text=True, timeout=20)

    return run, remote, tmp_path


def test_dry_run_recovers_only_requested_date(recovery):
    run, remote, root = recovery
    (remote / '20260927').mkdir()
    (remote / f'{DATE}_trade_ticks.tar.gz').write_bytes(b'stale archive')
    result = run()
    assert result.returncode == 0, result.stderr
    assert f'contracts/{DATE}/{DATE}_trade_ticks.tar.gz' in result.stdout
    assert not (root / 'upload').exists()
    with tarfile.open(root / f'work/data/{DATE}_trade_ticks.tar.gz') as archive:
        assert archive.getnames() == [DATE, f'{DATE}/symbolupdate',
                                      f'{DATE}/symbolupdate/part.parquet']


def test_helper_is_read_only_and_cleaned_up(recovery):
    run, _, root = recovery
    result = run(HELPER='true')
    assert result.returncode == 0, result.stderr
    spec = json.loads((root / 'manifest').read_text())['spec']
    assert spec['volumes'][0]['persistentVolumeClaim'] == {
        'claimName': 'tickrecorder-data', 'readOnly': True}
    calls = [json.loads(line) for line in (root / 'calls').read_text().splitlines()]
    assert calls[-1][:2] == ['delete', 'pod']


@pytest.mark.parametrize('strict, succeeds', [('true', False), ('false', True)])
def test_unfinished_files(recovery, strict, succeeds):
    run, remote, root = recovery
    (remote / DATE / 'symbolupdate/.part.inprogress').write_bytes(b'unfinished')
    result = run(STRICT=strict, HELPER='true')
    assert (result.returncode == 0) == succeeds, result.stderr
    assert 'delete' in (root / 'calls').read_text().splitlines()[-1]
    if succeeds:
        with tarfile.open(root / f'work/data/{DATE}_trade_ticks.tar.gz') as archive:
            assert all(not name.endswith('.inprogress') for name in archive.getnames())


@pytest.mark.parametrize('corrupt', ['false', 'true'])
def test_upload_and_checksum_verification(recovery, corrupt):
    run, _, root = recovery
    result = run(DRY_RUN='false', CORRUPT=corrupt)
    assert (result.returncode == 0) == (corrupt == 'false'), result.stderr
    assert json.loads((root / 'upload').read_text()) == {
        'bucket': 'index-bucket',
        'key': f'index-bucket-holder/contracts/{DATE}/{DATE}_trade_ticks.tar.gz',
    }
    if corrupt == 'true':
        assert 'Read-back checksum/size mismatch' in result.stderr


def test_archive_only_recovery(recovery):
    run, remote, _ = recovery
    part = remote / DATE / 'symbolupdate/part.parquet'
    with tarfile.open(remote / f'{DATE}_trade_ticks.tar.gz', 'w:gz') as archive:
        archive.add(part, arcname=f'{DATE}/symbolupdate/part.parquet')
    part.unlink()
    part.parent.rmdir()
    part.parent.parent.rmdir()
    result = run()
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('date', ['20260230', '2026-09-28', '20260927'])
def test_invalid_or_missing_date_does_not_upload(recovery, date):
    run, _, root = recovery
    result = run(date, DRY_RUN='false')
    assert result.returncode != 0
    assert not (root / 'upload').exists()


def test_archive_over_default_multipart_threshold_uses_single_upload(recovery):
    run, remote, root = recovery
    # Incompressible data keeps the resulting gzip above boto3's 8 MiB default.
    (remote / DATE / 'symbolupdate/part.parquet').write_bytes(os.urandom(9 * 1024 * 1024))
    result = run(DRY_RUN='false')
    assert result.returncode == 0, result.stderr
    assert (root / f'work/data/{DATE}_trade_ticks.tar.gz').stat().st_size > 8 * 1024 * 1024
    assert (root / 'upload').exists()
    assert 'sha256=' in result.stdout
