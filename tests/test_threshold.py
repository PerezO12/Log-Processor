"""Tests de threshold con resolver inyectado (DIP)."""
from processor.threshold import TemplateFrequency, detect_anomalies


def test_clear_spike_is_detected():
    cur = [TemplateFrequency("svc", 1, "tpl", 200)]
    hist = [[TemplateFrequency("svc", 1, "tpl", n)] for n in
            [10, 11, 9, 12, 10, 11, 9, 10, 11, 12]]
    out = detect_anomalies(cur, hist, lambda s: (3.0, 10))
    assert len(out) == 1
    assert out[0].direction == "up"
    assert out[0].z_score > 3


def test_no_anomaly_if_insufficient_history():
    cur = [TemplateFrequency("svc", 1, "tpl", 200)]
    hist = [[TemplateFrequency("svc", 1, "tpl", 10)] for _ in range(5)]  # < min_obs
    assert detect_anomalies(cur, hist, lambda s: (3.0, 10)) == []


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
    resolver = lambda svc: (2.5, 10) if svc == "strict" else (10.0, 10)
    out = detect_anomalies(cur, hist, resolver)
    assert [a.service for a in out] == ["strict"]


def test_down_anomaly_direction():
    cur = [TemplateFrequency("svc", 1, "tpl", 0)]
    hist = [[TemplateFrequency("svc", 1, "tpl", n)] for n in
            [50, 48, 52, 51, 49, 50, 51, 49, 50, 51, 52]]
    out = detect_anomalies(cur, hist, lambda s: (3.0, 10))
    assert out and out[0].direction == "down"


def test_no_history_for_template_skipped():
    cur = [TemplateFrequency("svc", 99, "new_template", 1000)]
    hist = [[TemplateFrequency("svc", 1, "other", 10)] for _ in range(20)]
    assert detect_anomalies(cur, hist, lambda s: (3.0, 10)) == []
