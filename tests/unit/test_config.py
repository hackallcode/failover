import pytest

from failover.config import ConfigError, load
from failover.main import litestream_options
from tests.unit.config_files import BASE, write


def test_litestream_uses_the_agents_tls(tmp_path):
    config = load(write(tmp_path))
    assert litestream_options(config) == {"tls": True, "root-cas": ["/pki/ca.crt"], "client-cert": "/pki/node.crt",
                                          "client-key": "/pki/node.key"}


def test_service_is_projects_or_a_command(tmp_path):
    with pytest.raises(ConfigError):
        load(write(tmp_path, command=["app"]))
    with pytest.raises(ConfigError):
        load(write(tmp_path, projects=[]))


def test_cloudflare_needs_a_zone_and_a_token(tmp_path):
    with pytest.raises(ConfigError):
        load(write(tmp_path, dns={**BASE["dns"], "zone": ""}))
