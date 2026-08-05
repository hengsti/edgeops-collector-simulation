from edgeops_collector_simulation.config import Settings
from edgeops_collector_simulation.simulation.profile import load_profile


def test_checked_in_profile_matches_simulation_allowlist(settings: Settings) -> None:
    profile = load_profile(
        settings.profile_path,
        allowed_services=settings.allowed_service_names,
    )
    assert frozenset(profile.services) == settings.allowed_service_names
    assert 0 <= profile.queue_initial_depth <= profile.queue_capacity
