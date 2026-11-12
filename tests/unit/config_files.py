import json

BASE = {
    "service": "app", "node": "a",
    "nats": {"servers": ["nats://10.0.0.1:4222"], "litestream_url": "nats://10.0.0.1:4222",
             "tls": {"ca": "/pki/ca.crt", "cert": "/pki/node.crt", "key": "/pki/node.key"}},
    "projects": ["/opt/app"], "databases": {"main": "/opt/app/db.sqlite"},
    "dns": {"provider": "cloudflare", "zone": "example.com", "names": ["app.example.com"], "ip": "192.0.2.1",
            "token_file": "/etc/failover/token"},
    "state_dir": "/var/lib/failover/app",
}


def write(tmp_path, **changes):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({**BASE, **changes}))
    return path
