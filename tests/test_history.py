"""Tests del store SQLite de historico."""
from datetime import datetime, timedelta, timezone

import pytest

from processor.history import HistoryStore
from processor.settings import HistoryConfig
from processor.threshold import TemplateFrequency


@pytest.fixture
def store(tmp_path):
    cfg = HistoryConfig(path=str(tmp_path / "h.db"))
    s = HistoryStore(cfg)
    yield s
    s.close()


def test_record_and_load_roundtrip(store):
    now = datetime.now(tz=timezone.utc)
    freqs = [
        TemplateFrequency("svc", 1, "tpl_a", 10),
        TemplateFrequency("svc", 2, "tpl_b", 5),
    ]
    store.record_window(now, freqs)
    windows = store.load_history("svc", days=1)
    assert len(windows) == 1
    assert {f.template_id for f in windows[0]} == {1, 2}


def test_survives_restart(tmp_path):
    cfg = HistoryConfig(path=str(tmp_path / "h.db"))
    now = datetime.now(tz=timezone.utc)
    s1 = HistoryStore(cfg)
    s1.record_window(now, [TemplateFrequency("svc", 1, "t", 42)])
    s1.close()

    # Simular restart: reabrir el store
    s2 = HistoryStore(cfg)
    windows = s2.load_history("svc", days=1)
    s2.close()
    assert len(windows) == 1
    assert windows[0][0].count == 42


def test_load_filters_by_service(store):
    now = datetime.now(tz=timezone.utc)
    store.record_window(now, [
        TemplateFrequency("a", 1, "t", 10),
        TemplateFrequency("b", 1, "t", 20),
    ])
    assert store.load_history("a", days=1)[0][0].count == 10
    assert store.load_history("b", days=1)[0][0].count == 20


def test_prune_removes_old(store):
    now = datetime.now(tz=timezone.utc)
    old = now - timedelta(days=10)
    store.record_window(old, [TemplateFrequency("svc", 1, "t", 5)])
    store.record_window(now, [TemplateFrequency("svc", 1, "t", 10)])
    deleted = store.prune(now - timedelta(days=7))
    assert deleted == 1
    windows = store.load_history("svc", days=30)
    assert len(windows) == 1


def test_record_empty_is_noop(store):
    store.record_window(datetime.now(tz=timezone.utc), [])
    assert store.load_history("svc", days=1) == []


def test_load_returns_windows_in_order(store):
    base = datetime.now(tz=timezone.utc)
    store.record_window(base - timedelta(minutes=10), [TemplateFrequency("s", 1, "t", 1)])
    store.record_window(base - timedelta(minutes=5), [TemplateFrequency("s", 1, "t", 2)])
    store.record_window(base, [TemplateFrequency("s", 1, "t", 3)])
    windows = store.load_history("s", days=1)
    counts = [w[0].count for w in windows]
    assert counts == [1, 2, 3]
