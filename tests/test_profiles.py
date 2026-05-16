"""Tests de los perfiles de parsing con muestras reales de trazas.md."""
import pytest

from processor.profiles import PROFILE_REGISTRY
from processor.settings import ProfileSpec


NESTJS_PATTERN = (
    r'^\[Nest\]\s+\d+.*?\s+'
    r'(?P<level>LOG|WARN|ERROR|DEBUG|VERBOSE)\s+'
    r'\[(?P<context>[^\]]+)\]\s+'
    r'(?P<message>.+)$'
)
NESTJS_LEVEL_MAP = {"LOG": "info", "WARN": "warn", "ERROR": "error",
                    "DEBUG": "debug", "VERBOSE": "debug"}


@pytest.fixture
def nestjs_profile():
    spec = ProfileSpec(type="regex", pattern=NESTJS_PATTERN, level_map=NESTJS_LEVEL_MAP)
    return PROFILE_REGISTRY["regex"].from_spec(spec)


@pytest.fixture
def kafkajs_profile():
    spec = ProfileSpec(
        type="json", level_field="level", message_field="message",
        extra_fields=["logger", "groupId"],
    )
    return PROFILE_REGISTRY["json"].from_spec(spec)


@pytest.fixture
def winston_profile():
    spec = ProfileSpec(
        type="json", level_field="level", message_field="message",
        extra_fields=["label"],
    )
    return PROFILE_REGISTRY["json"].from_spec(spec)


# --- NestJS regex ---------------------------------------------------------
def test_nestjs_warn(nestjs_profile):
    line = "[Nest] 117 - 05/13/2026, 3:13:59 PM    WARN [PaymentHandler] Region mismatch"
    r = nestjs_profile.parse(line)
    assert r.level == "warn"
    assert r.message == "Region mismatch"
    assert r.extras["context"] == "PaymentHandler"


def test_nestjs_error(nestjs_profile):
    line = "[Nest] 1 - 05/12/2026, 8:06:34 PM   ERROR [GrpcErrorHandler] gRPC error: bad"
    r = nestjs_profile.parse(line)
    assert r.level == "error"
    assert r.message == "gRPC error: bad"


def test_nestjs_log_mapped_to_info(nestjs_profile):
    line = "[Nest] 1 - 05/12/2026, 8:06:34 PM     LOG [Bootstrap] starting"
    r = nestjs_profile.parse(line)
    assert r.level == "info"


def test_nestjs_unmatched_line_is_unknown(nestjs_profile):
    r = nestjs_profile.parse("    at Object.callback (/usr/src/app/x.js:1:1)")
    assert r.level == "unknown"


def test_regex_requires_named_groups():
    bad_spec = ProfileSpec(type="regex", pattern=r"(?P<foo>.+)")
    with pytest.raises(ValueError):
        PROFILE_REGISTRY["regex"].from_spec(bad_spec)


# --- kafkajs JSON ---------------------------------------------------------
def test_kafkajs(kafkajs_profile):
    line = '{"level":"INFO","timestamp":"x","logger":"kafkajs","message":"joined","groupId":"g1"}'
    r = kafkajs_profile.parse(line)
    assert r.level == "info"
    assert r.message == "joined"
    assert r.extras == {"logger": "kafkajs", "groupId": "g1"}


def test_kafkajs_malformed(kafkajs_profile):
    r = kafkajs_profile.parse("not a json")
    assert r.level == "unknown"


# --- Winston JSON ---------------------------------------------------------
def test_winston(winston_profile):
    line = '{"label":"pgw","level":"info","message":"payment ok","timestamp":"x"}'
    r = winston_profile.parse(line)
    assert r.level == "info"
    assert r.message == "payment ok"
    assert r.extras == {"label": "pgw"}


def test_winston_missing_level_field(winston_profile):
    r = winston_profile.parse('{"message":"x"}')
    assert r.level == "unknown"


# --- Registry -------------------------------------------------------------
def test_registry_has_three_profiles():
    assert {"regex", "json", "fallback"}.issubset(PROFILE_REGISTRY.keys())


def test_same_json_class_two_specs():
    """Misma clase JsonProfile parametrizada para 2 formatos distintos."""
    a = PROFILE_REGISTRY["json"].from_spec(
        ProfileSpec(type="json", level_field="level", message_field="message")
    )
    b = PROFILE_REGISTRY["json"].from_spec(
        ProfileSpec(type="json", level_field="severity", message_field="text")
    )
    assert a.__class__ is b.__class__
    line = '{"severity":"warn","text":"hello"}'
    assert a.parse(line).level == "unknown"
    assert b.parse(line).level == "warn"
