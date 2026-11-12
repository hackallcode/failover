"""An A record in Cloudflare that names the serving node."""
import asyncio
import json
import urllib.request

API = "https://api.cloudflare.com/client/v4"


class CloudflareRecord:
    def __init__(self, token: str, zone: str, name: str, ip: str):
        self.token, self.zone, self.name, self.ip = token, zone, name, ip

    def call(self, method: str, path: str, body: dict | None = None):
        req = urllib.request.Request(API + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": f"Bearer {self.token}",
                                              "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.load(r)["result"]

    def point(self) -> None:
        zone_id = self.call("GET", f"/zones?name={self.zone}")[0]["id"]
        records = self.call("GET", f"/zones/{zone_id}/dns_records?type=A&name={self.name}")
        if records and records[0]["content"] == self.ip:
            return
        body = {"type": "A", "name": self.name, "content": self.ip, "ttl": records[0]["ttl"] if records else 60}
        if records:
            self.call("PATCH", f"/zones/{zone_id}/dns_records/{records[0]['id']}", body)
        else:
            self.call("POST", f"/zones/{zone_id}/dns_records", body)

    async def point_here(self) -> None:
        await asyncio.to_thread(self.point)


class Records:
    """Several names, e.g. a service on more than one domain."""

    def __init__(self, records: list):
        self.records = records

    async def point_here(self) -> None:
        for record in self.records:
            await record.point_here()
