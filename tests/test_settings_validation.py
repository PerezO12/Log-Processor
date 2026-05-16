"""Cross-validation: un service que apunte a un profile inexistente debe fallar."""
import pytest
from pydantic import ValidationError

from processor.settings import (
    ProcessorConfig,
    ProcessorDefaults,
    ProfileSpec,
    ServiceConfig,
    ServiceOverrides,
    Settings,
)


def _base_kwargs(**overrides):
    base = {
        "processor": ProcessorConfig(
            services=[ServiceConfig(name="svc1", profile="nonexistent")],
        ),
        "profiles": {},
    }
    base.update(overrides)
    return base


def test_unknown_profile_fails_fast():
    with pytest.raises(ValidationError) as exc:
        Settings(**_base_kwargs())
    assert "unknown profiles" in str(exc.value)


def test_known_profile_passes():
    s = Settings(
        processor=ProcessorConfig(
            services=[ServiceConfig(name="svc1", profile="nestjs")],
        ),
        profiles={"nestjs": ProfileSpec(type="fallback")},
    )
    assert s.resolve_service("svc1").profile_name == "nestjs"


def test_resolve_service_applies_overrides():
    s = Settings(
        processor=ProcessorConfig(
            defaults=ProcessorDefaults(threshold_k=3.0, min_observations=10),
            services=[
                ServiceConfig(
                    name="svc1",
                    profile="p",
                    overrides=ServiceOverrides(threshold_k=2.5, min_observations=5),
                ),
                ServiceConfig(name="svc2", profile="p"),
            ],
        ),
        profiles={"p": ProfileSpec(type="fallback")},
    )
    r1 = s.resolve_service("svc1")
    r2 = s.resolve_service("svc2")
    assert (r1.threshold_k, r1.min_observations) == (2.5, 5)
    assert (r2.threshold_k, r2.min_observations) == (3.0, 10)


def test_resolve_service_unknown_raises():
    s = Settings(profiles={"p": ProfileSpec(type="fallback")})
    with pytest.raises(KeyError):
        s.resolve_service("nope")


def test_enabled_services_excludes_disabled():
    s = Settings(
        processor=ProcessorConfig(
            services=[
                ServiceConfig(name="on", profile="p", enabled=True),
                ServiceConfig(name="off", profile="p", enabled=False),
            ],
        ),
        profiles={"p": ProfileSpec(type="fallback")},
    )
    assert [r.name for r in s.enabled_services()] == ["on"]
