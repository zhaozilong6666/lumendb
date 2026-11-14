"""复制流 TLS 冒烟测试（测试专用自签名证书）。"""

from __future__ import annotations

import ssl

import pytest
from pathlib import Path

from lumendb.engine.database import Database
from lumendb.replication import Replica, WALServer

HERE = Path(__file__).parent / "tls"


def _server_context() -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(HERE / "server.crt", HERE / "server.key")
    return context


def _client_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=str(HERE / "server.crt"))


def test_tls_backup_and_wal_stream(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    server = WALServer(primary, ssl_context=_server_context()).start()
    replica = None
    try:
        replica = Replica.bootstrap_from_server(
            server.host,
            server.port,
            tmp_path / "replica",
            ssl_context=_client_context(),
        )
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
        primary.execute_one("INSERT INTO t VALUES (2)")
        assert (
            replica.replicate_from_server(
                server.host,
                server.port,
                ssl_context=_client_context(),
            )
            > 0
        )
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(2,)]
    finally:
        if replica is not None:
            replica.db.close()
        server.stop()
        primary.close()


def test_mutual_tls_requires_client_certificate(tmp_path):
    server_context = _server_context()
    server_context.verify_mode = ssl.CERT_REQUIRED
    server_context.load_verify_locations(str(HERE / "server.crt"))
    client_context = _client_context()
    client_context.load_cert_chain(HERE / "server.crt", HERE / "server.key")

    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    server = WALServer(primary, ssl_context=server_context).start()
    replica = None
    try:
        replica = Replica.bootstrap_from_server(
            server.host,
            server.port,
            tmp_path / "replica",
            ssl_context=client_context,
        )
        assert replica.execute_one("SELECT COUNT(*) FROM t").rows == [(1,)]
    finally:
        if replica is not None:
            replica.db.close()
        server.stop()
        primary.close()


def test_tls_hostname_verification_rejects_wrong_name(tmp_path):
    primary = Database(tmp_path / "primary", checkpoint_wal_bytes=0)
    primary.execute_one("CREATE TABLE t (id INT PRIMARY KEY)")
    primary.execute_one("INSERT INTO t VALUES (1)")
    server = WALServer(primary, ssl_context=_server_context()).start()
    try:
        with pytest.raises(ssl.SSLCertVerificationError):
            Replica.bootstrap_from_server(
                server.host,
                server.port,
                tmp_path / "replica",
                ssl_context=_client_context(),
                server_hostname="wrong.example",
            )
    finally:
        server.stop()
        primary.close()
