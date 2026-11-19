import pytest

from taskmaster.jobs.retry import backoff_delay


@pytest.mark.parametrize(("attempt", "expected"), [(1, 2.0), (2, 4.0), (3, 8.0), (4, 16.0)])
def test_backoff_doubles(attempt: int, expected: float) -> None:
    assert backoff_delay(attempt, base=2.0, cap=300.0) == expected


def test_backoff_is_capped() -> None:
    assert backoff_delay(20, base=2.0, cap=60.0) == 60.0


def test_backoff_jitter_stays_within_bounds() -> None:
    delays = {backoff_delay(3, base=2.0, cap=300.0, jitter=0.1) for _ in range(200)}
    assert all(7.2 <= d <= 8.8 for d in delays)
    assert len(delays) > 1
