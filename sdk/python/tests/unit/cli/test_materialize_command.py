import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from feast import utils
from feast.cli.cli import materialize_command


@pytest.fixture
def non_utc_local_tz(monkeypatch):
    """Run the test with a local timezone that is always behind UTC."""
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset() is not available on Windows")
    monkeypatch.setenv("TZ", "America/Los_Angeles")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def test_materialize_disable_event_timestamp_uses_utc_bounds(mocker, non_utc_local_tz):
    mock_store = MagicMock()
    mocker.patch("feast.cli.cli.create_feature_store", return_value=mock_store)

    result = CliRunner().invoke(materialize_command, ["--disable-event-timestamp"])
    assert result.exit_code == 0, result.output

    mock_store.materialize.assert_called_once()
    kwargs = mock_store.materialize.call_args.kwargs
    start_date, end_date = kwargs["start_date"], kwargs["end_date"]

    # FeatureStore.materialize treats naive datetimes as UTC, so the end bound
    # must be the current UTC time, not the host's local wall-clock time.
    drift = utils.make_tzaware(end_date) - datetime.now(timezone.utc)
    assert abs(drift) < timedelta(seconds=30)
    assert end_date.tzinfo is not None
    assert start_date == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert kwargs["disable_event_timestamp"] is True
