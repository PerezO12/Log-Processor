"""Tests de threshold con resolver inyectado (DIP)."""
from processor.threshold import TemplateFrequency, ThresholdParams, detect_anomalies


def _params(k: float, min_obs: int = 10) -> ThresholdParams:
    return ThresholdParams(k=k, min_observations=min_obs, min_count=0.0, min_delta=0)


def test_clear_spike_is_detected():
    cur = [TemplateFrequency("svc", 1, "tpl", 200)]
    hist = [[TemplateFrequency("svc", 1, "tpl", n)] for n in
            [10, 11, 9, 12, 10, 11, 9, 10, 11, 12]]
    out = detect_anomalies(cur, hist, lambda s: _params(3.0))
    assert len(out) == 1
    assert out[0].direction == "up"
    assert out[0].z_score > 3


def test_no_anomaly_if_insufficient_history():
    cur = [TemplateFrequency("svc", 1, "tpl", 200)]
    hist = [[TemplateFrequency("svc", 1, "tpl", 10)] for _ in range(5)]  # < min_obs
    assert detect_anomalies(cur, hist, lambda s: _params(3.0)) == []


def test_resolver_discriminates_services():
    """Mismo count + mismo histo, distinto k segun servicio -> distintos veredictos."""
    samples = [8, 9, 10, 11, 12, 10, 9, 11, 10, 12, 9, 10, 11, 10, 9]
    cur = [
        TemplateFrequency("strict", 1, "t", 15),
        TemplateFrequency("loose", 2, "t", 15),
    ]
    hist = [
        [TemplateFrequency("strict", 1, "t", s),
         TemplateFrequency("loose", 2, "t", s)]
        for s in samples
    ]
    resolver = lambda svc: _params(2.5) if svc == "strict" else _params(10.0)
    out = detect_anomalies(cur, hist, resolver)
    assert [a.service for a in out] == ["strict"]


def test_down_anomaly_direction():
    cur = [TemplateFrequency("svc", 1, "tpl", 0)]
    hist = [[TemplateFrequency("svc", 1, "tpl", n)] for n in
            [50, 48, 52, 51, 49, 50, 51, 49, 50, 51, 52]]
    out = detect_anomalies(cur, hist, lambda s: _params(3.0))
    assert out and out[0].direction == "down"


def test_no_history_for_template_skipped():
    cur = [TemplateFrequency("svc", 99, "new_template", 1000)]
    hist = [[TemplateFrequency("svc", 1, "other", 10)] for _ in range(20)]
    assert detect_anomalies(cur, hist, lambda s: _params(3.0)) == []


def test_min_delta_suppresses_trivial_change():
    """Cambio absoluto |current - mean| menor que min_delta -> sin anomalia."""
    cur = [TemplateFrequency("svc", 1, "tpl", 3)]
    hist = [[TemplateFrequency("svc", 1, "tpl", 2)] for _ in range(15)]
    # sigma=0 historica + cambio=1 + min_delta=2 -> debe filtrarse
    out = detect_anomalies(
        cur, hist,
        lambda s: ThresholdParams(k=3.0, min_observations=10, min_count=0.0, min_delta=2),
    )
    assert out == []


def test_sentinel_z_for_constant_history():
    """Frecuencia historica constante (sigma=0) + cambio -> z=+/-1000 (sentinel)."""
    cur = [TemplateFrequency("svc", 1, "tpl", 5)]
    hist = [[TemplateFrequency("svc", 1, "tpl", 2)] for _ in range(15)]
    out = detect_anomalies(cur, hist, lambda s: _params(3.0))
    assert len(out) == 1
    assert out[0].z_score == 1000.0
    assert out[0].direction == "up"
