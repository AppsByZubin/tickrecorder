from __future__ import annotations

import io
import tarfile
from pathlib import Path
from typing import Any

import pytest

from tickrecorder.spaces import (
    DigitalOceanSpacesConfig,
    create_trade_ticks_archive,
    finalized_trading_dates,
    normalize_do_spaces_endpoint_url,
    normalize_s3_key,
    pending_trading_dates,
    upload_trade_ticks_archive,
    write_upload_receipt_atomic,
)


class FakeS3Client:
    def __init__(
        self,
        remote_size_delta: int = 0,
        corrupt_remote_same_size: bool = False,
    ) -> None:
        self.remote_size_delta = remote_size_delta
        self.corrupt_remote_same_size = corrupt_remote_same_size
        self.uploads: list[tuple[Path, str, str]] = []
        self._uploaded_bytes = b""
        self._metadata: dict[str, str] = {}

    def upload_file(
        self,
        local_path: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, Any],
    ) -> None:
        path = Path(local_path)
        self.uploads.append((path, bucket, key))
        self._uploaded_bytes = path.read_bytes()
        assert ExtraArgs["ContentType"] == "application/gzip"
        self._metadata = ExtraArgs["Metadata"]

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        assert self.uploads[-1][1:] == (Bucket, Key)
        return {
            "ContentLength": len(self._uploaded_bytes) + self.remote_size_delta,
            "ETag": '"fake-etag"',
            "Metadata": self._metadata,
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        assert self.uploads[-1][1:] == (Bucket, Key)
        body = self._uploaded_bytes
        if self.corrupt_remote_same_size and body:
            body = bytes([body[0] ^ 0xFF]) + body[1:]
        return {"Body": io.BytesIO(body)}


def spaces_config() -> DigitalOceanSpacesConfig:
    return DigitalOceanSpacesConfig(
        endpoint_url="https://sgp1.digitaloceanspaces.com",
        region="sgp1",
        bucket_name="index-bucket",
        prefix="index-bucket-holder/contracts",
        access_key_id="access-key",
        secret_access_key="secret-key",
    )


def create_date_tree(data_dir: Path, trading_date: str = "20260717") -> Path:
    date_dir = data_dir / trading_date
    for stream in ("control", "symbolupdate", "tbtdepth"):
        stream_dir = date_dir / stream
        stream_dir.mkdir(parents=True)
        (stream_dir / f"part-{stream}.parquet").write_bytes(stream.encode())
    return date_dir


def test_archive_contains_date_directory_and_all_stream_files(tmp_path: Path) -> None:
    create_date_tree(tmp_path)
    run_dir = tmp_path / "_runs" / "run-id"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    artifact = create_trade_ticks_archive(tmp_path, "20260717")

    assert artifact.archive_path == tmp_path / "20260717_trade_ticks.tar.gz"
    assert artifact.file_count == 3
    assert artifact.source_fingerprint
    with tarfile.open(artifact.archive_path, "r:gz") as archive:
        names = set(archive.getnames())
    assert names == {
        "20260717",
        "20260717/control",
        "20260717/control/part-control.parquet",
        "20260717/symbolupdate",
        "20260717/symbolupdate/part-symbolupdate.parquet",
        "20260717/tbtdepth",
        "20260717/tbtdepth/part-tbtdepth.parquet",
    }
    assert not any("_runs" in name for name in names)
    assert not any("trade_ticks.tar.gz" in name for name in names)


def test_archive_rejects_unfinished_files_and_preserves_previous_archive(
    tmp_path: Path,
) -> None:
    date_dir = create_date_tree(tmp_path)
    archive_path = tmp_path / "20260717_trade_ticks.tar.gz"
    archive_path.write_bytes(b"previous-good-archive")
    (date_dir / "control" / ".part.inprogress").write_bytes(b"partial")

    with pytest.raises(RuntimeError, match="unfinished file"):
        create_trade_ticks_archive(tmp_path, "20260717")

    assert archive_path.read_bytes() == b"previous-good-archive"
    assert not list(tmp_path.glob(".*.inprogress"))


@pytest.mark.parametrize("trading_date", ["2026-07-17", "../20260717", ""])
def test_archive_rejects_invalid_dates(tmp_path: Path, trading_date: str) -> None:
    with pytest.raises(ValueError, match="Invalid trading date"):
        create_trade_ticks_archive(tmp_path, trading_date)


def test_finalized_dates_are_unique_sorted_and_validated() -> None:
    parts = [
        {"path": "20260718/tbtdepth/part-2.parquet"},
        {"path": "20260717/control/part-1.parquet"},
        {"path": "20260718/symbolupdate/part-3.parquet"},
    ]

    assert finalized_trading_dates(parts) == ("20260717", "20260718")

    with pytest.raises(RuntimeError, match="invalid trading-date"):
        finalized_trading_dates([{"path": "_runs/run/manifest.json"}])


def test_spaces_endpoint_and_object_key_normalization() -> None:
    assert (
        normalize_do_spaces_endpoint_url(
            "index-bucket.sgp1.digitaloceanspaces.com/path",
            "sgp1",
            "index-bucket",
        )
        == "https://sgp1.digitaloceanspaces.com"
    )
    assert (
        normalize_s3_key("index-bucket", "/index-bucket/path/to/object")
        == "path/to/object"
    )
    assert (
        normalize_do_spaces_endpoint_url(
            "https://sgp1.digitaloceanspaces.com/unwanted/path?query=yes",
            "sgp1",
            "index-bucket",
        )
        == "https://sgp1.digitaloceanspaces.com"
    )


def test_verified_receipt_removes_retained_date_from_pending_set(
    tmp_path: Path,
) -> None:
    create_date_tree(tmp_path)
    assert pending_trading_dates(tmp_path) == ("20260717",)

    artifact = create_trade_ticks_archive(tmp_path, "20260717")
    receipt_path = write_upload_receipt_atomic(
        tmp_path,
        "20260717",
        {
            "sha256": artifact.sha256,
            "source_fingerprint": artifact.source_fingerprint,
            "bucket_name": "index-bucket",
            "object_key": (
                "index-bucket-holder/contracts/20260717/"
                "20260717_trade_ticks.tar.gz"
            ),
        },
    )

    assert receipt_path.is_file()
    assert pending_trading_dates(tmp_path) == ()
    assert pending_trading_dates(tmp_path, prefix="different/contracts") == (
        "20260717",
    )

    (tmp_path / "20260717" / "control" / "later-part.parquet").write_bytes(
        b"later ticks"
    )
    assert pending_trading_dates(tmp_path) == ("20260717",)

    receipt_path.write_text("not-json", encoding="utf-8")
    assert pending_trading_dates(tmp_path) == ("20260717",)


def test_upload_uses_exact_destination_and_verifies_remote_size(
    tmp_path: Path,
) -> None:
    create_date_tree(tmp_path)
    artifact = create_trade_ticks_archive(tmp_path, "20260717")
    client = FakeS3Client()

    receipt = upload_trade_ticks_archive(
        artifact,
        spaces_config(),
        client=client,
    )

    expected_key = (
        "index-bucket-holder/contracts/20260717/"
        "20260717_trade_ticks.tar.gz"
    )
    assert client.uploads == [
        (artifact.archive_path, "index-bucket", expected_key)
    ]
    assert receipt.object_key == expected_key
    assert receipt.uri == f"s3://index-bucket/{expected_key}"
    assert receipt.size_bytes == artifact.size_bytes
    assert receipt.sha256 == artifact.sha256
    assert receipt.etag == "fake-etag"


def test_upload_rejects_remote_size_mismatch(tmp_path: Path) -> None:
    create_date_tree(tmp_path)
    artifact = create_trade_ticks_archive(tmp_path, "20260717")

    with pytest.raises(RuntimeError, match="size mismatch"):
        upload_trade_ticks_archive(
            artifact,
            spaces_config(),
            client=FakeS3Client(remote_size_delta=1),
        )


def test_upload_rejects_same_size_checksum_mismatch(tmp_path: Path) -> None:
    create_date_tree(tmp_path)
    artifact = create_trade_ticks_archive(tmp_path, "20260717")

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        upload_trade_ticks_archive(
            artifact,
            spaces_config(),
            client=FakeS3Client(corrupt_remote_same_size=True),
        )


def test_upload_builds_digitalocean_virtual_host_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_date_tree(tmp_path)
    artifact = create_trade_ticks_archive(tmp_path, "20260717")
    client = FakeS3Client()
    captured_kwargs: dict[str, Any] = {}

    def fake_boto_client(service: str, **kwargs: Any) -> FakeS3Client:
        assert service == "s3"
        captured_kwargs.update(kwargs)
        return client

    monkeypatch.setattr("tickrecorder.spaces.boto3.client", fake_boto_client)

    upload_trade_ticks_archive(artifact, spaces_config())

    assert captured_kwargs["region_name"] == "sgp1"
    assert captured_kwargs["endpoint_url"] == (
        "https://sgp1.digitaloceanspaces.com"
    )
    assert captured_kwargs["aws_access_key_id"] == "access-key"
    assert captured_kwargs["aws_secret_access_key"] == "secret-key"
    assert captured_kwargs["config"].s3 == {"addressing_style": "virtual"}
    assert captured_kwargs["config"].connect_timeout == 10
    assert captured_kwargs["config"].read_timeout == 60


def test_spaces_config_repr_does_not_expose_credentials() -> None:
    rendered = repr(spaces_config())
    assert "access-key" not in rendered
    assert "secret-key" not in rendered
