import sqlite3
import time
from datetime import datetime, timezone
from typing import Union

import pytest

from feast import Entity, FeatureView, Field, FileSource
from feast.infra.online_stores.sqlite import (
    SqliteOnlineStore,
    SqliteOnlineStoreConfig,
    _table_id,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig
from feast.types import Int64
from feast.value_type import ValueType

ENTITY_KEY = EntityKeyProto(
    join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
)
FEATURES = {"trips": ValueProto(int64_val=7)}


@pytest.fixture
def london_local_tz(monkeypatch):
    """Run the test with a local timezone that observes daylight saving time."""
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset() is not available on Windows")
    monkeypatch.setenv("TZ", "Europe/London")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


@pytest.fixture
def sqlite_store(tmp_path):
    config = RepoConfig(
        project="test_project",
        provider="local",
        registry=RegistryConfig(path=str(tmp_path / "registry.db")),
        online_store=SqliteOnlineStoreConfig(path=str(tmp_path / "online.db")),
        entity_key_serialization_version=3,
        repo_path=tmp_path,
    )
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    feature_view = FeatureView(
        name="driver_stats",
        entities=[driver],
        schema=[Field(name="driver_id", dtype=Int64), Field(name="trips", dtype=Int64)],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
    )
    store = SqliteOnlineStore()
    store.update(config, [], [feature_view], [], [driver], partial=False)
    return store, config, feature_view


def _write_then_read(sqlite_store, event_ts: datetime) -> datetime:
    store, config, feature_view = sqlite_store
    store.online_write_batch(
        config, feature_view, [(ENTITY_KEY, FEATURES, event_ts, None)], None
    )
    read_ts, values = store.online_read(config, feature_view, [ENTITY_KEY])[0]
    assert values == FEATURES
    return read_ts


def _overwrite_event_ts_then_read(sqlite_store, raw_event_ts: Union[int, str]):
    """Store a raw event_ts value the way an earlier Feast version did, then read."""
    store, config, feature_view = sqlite_store
    _write_then_read(sqlite_store, datetime(2020, 1, 1, tzinfo=timezone.utc))
    table_name = _table_id(config.project, feature_view)
    conn = sqlite3.connect(config.online_store.path)
    with conn:
        conn.execute(f'UPDATE "{table_name}" SET event_ts = ?', (raw_event_ts,))
    conn.close()
    read_ts, _ = store.online_read(config, feature_view, [ENTITY_KEY])[0]
    return read_ts


def test_event_timestamp_round_trip_keeps_microseconds(sqlite_store):
    event_ts = datetime(2026, 7, 1, 12, 0, 0, 750123, tzinfo=timezone.utc)
    assert _write_then_read(sqlite_store, event_ts) == event_ts


def test_event_timestamp_round_trip_in_local_dst_gap(sqlite_store, london_local_tz):
    # 01:30 on 2026-03-29 does not exist as a Europe/London wall-clock time.
    event_ts = datetime(2026, 3, 29, 1, 30, tzinfo=timezone.utc)
    assert _write_then_read(sqlite_store, event_ts) == event_ts


def test_legacy_iso_text_event_timestamp_is_readable(sqlite_store):
    # Feast < 0.42 relied on Python's default sqlite3 datetime adapter.
    read_ts = _overwrite_event_ts_then_read(sqlite_store, "2026-07-01 12:00:00.250000")
    assert read_ts == datetime(2026, 7, 1, 12, 0, 0, 250000, tzinfo=timezone.utc)


def test_legacy_integer_epoch_event_timestamp_is_readable(
    sqlite_store, london_local_tz
):
    # The previous epoch adapter stored int(naive_utc.timestamp()), which
    # interprets the naive UTC value in the host's local timezone.
    legacy_epoch = int(datetime(2026, 7, 1, 12, 0, 0).timestamp())
    read_ts = _overwrite_event_ts_then_read(sqlite_store, legacy_epoch)
    assert read_ts == datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
