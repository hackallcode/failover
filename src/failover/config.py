"""The agent's configuration file (JSON), checked once on load."""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Tls:
    ca: Path
    cert: Path
    key: Path


@dataclass(frozen=True)
class Nats:
    servers: list[str]
    replicas: int = 3
    user: str | None = None
    password_file: Path | None = None
    tls: Tls | None = None
    server_name: str = ""


@dataclass(frozen=True)
class Dns:
    provider: Literal["cloudflare", "http"]
    names: list[str]
    ip: str
    zone: str = ""
    token_file: Path | None = None
    url: str = ""


@dataclass(frozen=True)
class Api:
    listen: str
    token_file: Path


@dataclass(frozen=True)
class Config:
    service: str
    node: str
    nats: Nats
    projects: list[Path]
    command: list[str]
    databases: dict[str, Path]
    dns: Dns
    state_dir: Path
    candidate: bool = True
    lease_ttl: float = 15
    tick: float = 1
    desired_valid: float = 120
    yield_for: float = 15
    place_streams: bool = True
    hold_file: Path | None = None
    api: Api | None = None
    metrics_dir: Path | None = None


def load(path: Path) -> Config:
    try:
        raw = json.loads(path.read_text())
        nats = Nats(servers=list(raw["nats"]["servers"]),
                    replicas=int(raw["nats"].get("replicas", 3)), user=raw["nats"].get("user"),
                    password_file=_path(raw["nats"].get("password_file")), server_name=raw["nats"].get("server_name", ""),
                    tls=Tls(ca=Path(raw["nats"]["tls"]["ca"]), cert=Path(raw["nats"]["tls"]["cert"]),
                            key=Path(raw["nats"]["tls"]["key"])) if raw["nats"].get("tls") else None)
        dns = Dns(provider=raw["dns"]["provider"], names=list(raw["dns"]["names"]), ip=raw["dns"]["ip"],
                  zone=raw["dns"].get("zone", ""), token_file=_path(raw["dns"].get("token_file")),
                  url=raw["dns"].get("url", ""))
        api = Api(listen=raw["api"]["listen"], token_file=Path(raw["api"]["token_file"])) if raw.get("api") else None
        config = Config(
            service=raw["service"], node=raw["node"], nats=nats, projects=[Path(p) for p in raw.get("projects", [])],
            command=list(raw.get("command", [])),
            databases={name: Path(p) for name, p in raw["databases"].items()}, dns=dns,
            state_dir=Path(raw["state_dir"]), candidate=bool(raw.get("candidate", True)),
            lease_ttl=float(raw.get("lease_ttl", 15)), tick=float(raw.get("tick", 1)),
            desired_valid=float(raw.get("desired_valid", 120)), yield_for=float(raw.get("yield_for", 15)),
            place_streams=bool(raw.get("place_streams", True)), hold_file=_path(raw.get("hold_file")), api=api,
            metrics_dir=_path(raw.get("metrics_dir")))
    except (OSError, ValueError, KeyError, TypeError) as e:
        raise ConfigError(f"{path}: {e!r}") from e
    if dns.provider not in ("cloudflare", "http"):
        raise ConfigError(f"{path}: unknown dns provider {dns.provider!r}")
    if dns.provider == "cloudflare" and not (dns.zone and dns.token_file):
        raise ConfigError(f"{path}: cloudflare needs zone and token_file")
    if bool(config.projects) == bool(config.command):
        raise ConfigError(f"{path}: give either projects or command")
    if not config.databases:
        raise ConfigError(f"{path}: no databases")
    return config


def _path(value: str | None) -> Path | None:
    return Path(value) if value else None
