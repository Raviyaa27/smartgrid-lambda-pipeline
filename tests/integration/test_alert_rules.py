"""
The Prometheus alert rules, checked and unit-tested by promtool inside the
Prometheus image -- the same binary that will evaluate them.

The rule tests themselves are in infra/prometheus/tests/rules_test.yml; this
runs them with the rest of the suite. Skipped without Docker.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

PROMETHEUS = Path(__file__).resolve().parents[2] / "infra" / "prometheus"
IMAGE = "prom/prometheus:v3.5.1"


def promtool(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "run", "--rm", "--entrypoint", "promtool",
         "-v", f"{PROMETHEUS}:/etc/prometheus:ro", IMAGE, *args],
        capture_output=True,
        text=True,
        timeout=120,
    )  # fmt: skip


@pytest.fixture(scope="module", autouse=True)
def require_docker():
    if shutil.which("docker") is None:
        pytest.skip("docker is not installed")
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        pytest.skip("the Docker engine is not running")
    if subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode:
        pytest.skip(f"{IMAGE} is not pulled; run `docker compose pull prometheus`")


def test_the_configuration_and_rules_are_valid():
    result = promtool("check", "config", "/etc/prometheus/prometheus.yml")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "8 rules found" in result.stdout


def test_every_rule_fires_when_it_should_and_not_when_it_should_not():
    result = promtool("test", "rules", "/etc/prometheus/tests/rules_test.yml")
    assert result.returncode == 0, result.stdout + result.stderr
