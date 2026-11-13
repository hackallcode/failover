import pytest

from failover.config import ConfigError, load
from tests.unit.config_files import BASE, write


def test_service_is_projects_or_a_command(tmp_path):
    with pytest.raises(ConfigError):
        load(write(tmp_path, command=["app"]))
    with pytest.raises(ConfigError):
        load(write(tmp_path, projects=[]))


def test_cloudflare_needs_a_zone_and_a_token(tmp_path):
    with pytest.raises(ConfigError):
        load(write(tmp_path, dns={**BASE["dns"], "zone": ""}))
