import pytest
from pydantic import ValidationError

from edgeops_collector_simulation.config import Settings, validate_environment


def test_keys_must_be_distinct_and_are_redacted() -> None:
    secret = "same-sensitive-key"
    with pytest.raises(ValidationError) as exc_info:
        Settings(api_key=secret, admin_api_key=secret)
    assert secret not in str(exc_info.value)


@pytest.mark.parametrize("name", ["SIMULATION_COLLECTOR_ADMN_API_KEY", "COLLECTOR_MODE"])
def test_old_or_misspelled_environment_is_rejected(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    monkeypatch.setenv(name, "sensitive-value")
    with pytest.raises(ValueError, match=name) as exc_info:
        validate_environment()
    assert "sensitive-value" not in str(exc_info.value)
