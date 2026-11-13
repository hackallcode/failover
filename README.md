# failover

One writer at a time for services that keep their state in SQLite, across a few servers, with NATS JetStream as
the only quorum. Each server runs an agent per service; the agent that holds the service's lease runs it, the others
keep a standby copy of its databases and stay stopped.

- **Election**: a key in the JetStream KV bucket `failover`; each write expires after the writer's lease TTL. Every
  write compares the key's revision, so only the holder renews it; the term is the revision that created it.
  Agents learn the holder and switch requests from a watch on their keys rather than reading them every tick: any
  replica answers a read, so each would cross a route. A lease unrenewed for a TTL counts as expired, and a lease
  is taken only after a read from the stream leader.
- **Fencing**: a leader stops the service once 70% of the TTL passed since its last confirmed renewal was sent. The
  key expires on the server no earlier than a full TTL after that send, so no other node can hold it while the old
  leader still serves.
- **State**: the leader checks its databases every second and, on a change, writes the pages that differ from the
  database's last compressed image in the same bucket; once they outweigh a quarter of the image, or the image is a
  day old, it writes a new image instead. Every write compares the key's revision, so a node that lost the lease
  cannot overwrite a newer one. Standbys watch the bucket and rebuild their copy from each image and delta as it
  arrives; nothing polls.
  The leader records the newest replicated revisions (the position) in KV; a new leader starts only from a
  database that reaches it: its own, if it wrote the position and is not behind it; a standby copy that caught up;
  or the image and delta in the bucket. Meant for small databases: an image travels as one NATS message of at most
  1 MB.
- **Traffic**: the leader points DNS records at itself (Cloudflare).
- **Manual switch**: a request names the node to lead; the leader stops, ships the rest, records the position and
  frees the lease; the requested node takes it.

## Guarantees and limits

- Two nodes never serve the same service at once while the clocks on a node advance at about the same rate.
- Replication is asynchronous: a leader that dies loses what changed since it last shipped (up to a second). A
  leader cut off from the quorum keeps serving whoever can still reach it until it fences, and those writes are
  lost: up to 70% of the TTL.
- A node never serves a database behind the recorded position. When no node can reach it (the replica is short
  and no standby has it), nobody takes over until an operator does.
- Without a quorum nothing changes hands, and the leader fences.
- The leader reports how long its changes have not reached the replica (`failover_replication_lag_seconds`).
- A node frozen past its lease (a paused VM, SIGSTOP) may write for a moment after it wakes up, before its agent
  notices and stops the service; those writes are lost. Applications that cannot check a fencing token cannot
  rule this out.

## Requirements

- NATS server 2.11+ (per-message TTL) with JetStream, three or more servers. Set `ping_interval: "2s"` and
  `ping_max: 3`: by default a server that died without closing its connections is noticed only after minutes, and
  requests that land on it time out meanwhile.
- Python 3.11+.

## Configuration

One JSON file per service and node:

```json
{
  "service": "app",
  "node": "a",
  "nats": {"servers": ["nats://127.0.0.1:4222", "nats://b:4222", "nats://c:4222"], "replicas": 3},
  "projects": ["/opt/app", "/opt/app-panel"],
  "databases": {"main": "/opt/app/data/db.sqlite", "panel": "/opt/app-panel/data/panel.db"},
  "dns": {"provider": "cloudflare", "zone": "example.com", "names": ["app.example.com"], "ip": "192.0.2.1",
          "token_file": "/etc/failover/cloudflare-token"},
  "lease_ttl": 15,
  "hold_file": "/var/lib/failover/app/hold",
  "api": {"listen": "127.0.0.1:8480", "token_file": "/etc/failover/api-token"},
  "metrics_dir": "/var/lib/node_exporter"
}
```

- `projects`: docker compose projects started in order and stopped in reverse; or `command`, one process.
- `databases`: name → path; the name is stable across nodes, and the database's keys in the bucket are
  `<service>.snapshot.<name>` (the image) and `<service>.snapshot.<name>.delta` (the pages changed since).
- `nats`: `servers`, tried in order: put this node's own server first, so losing another server does not cut
  the leader off; `user` and `password_file` if the servers require them, `tls` (`ca`, `cert`, `key`) for client
  certificates, `server_name` if this node's NATS server is named differently from `node`.
- `lease_ttl`: seconds; each lease write expires after the writer's TTL, rounded up to whole seconds, so changing
  it on a running cluster is safe.
- `hold_file`: while it exists (and is younger than 5 minutes) the leader keeps the lease but neither restarts the
  service nor hands it over, for a deploy that edits the stopped database.
- `candidate: false`: follow and report, never lead.
- `place_streams` (`true`): the leader pulls the leader of the bucket's stream onto its own server every 30 s, so
  renewals and replica writes commit without a hop to a busy server; `false` on a node too loaded to lead them.
  All services share that stream: with several services, leave it on for one of them only.
- `tick` (1 s): how often the agent checks the lease, the request and the service.
- `desired_valid` (120 s): how long a switch request stands; meanwhile no other node campaigns.
- `yield_for` (15 s): after handing over, failing to take over or fencing, the node does not campaign for this
  long, so a healthy node takes over first.

## Running

```
failover run <config>             # the agent
failover stop <config>            # stop the service, e.g. ExecStopPost
failover request <config> <node>  # ask <node> to lead
failover state <config>           # leader, request, position, history
```

`state` and `request` only read and write the service's keys; they never create or change the bucket. A restarted
agent frees a lease left under its own name and campaigns again, so a leader's restart keeps it leading; a node
that stays down leaves the lease to run out, and a standby takes over then.

Run it under systemd as `Type=notify` with `WatchdogSec` and `ExecStopPost=failover stop <config>`: the service's
containers outlive the agent, and a dead or hung agent must not leave them writing without the lease.

The API (`api` in the config) answers `GET /state` and `POST /takeover` (a request for this node to lead) with a
bearer token.

## Upgrading from 0.1.0

0.1.0 kept the lease in a bucket per service and the databases in Litestream object stores; the position it
recorded counts Litestream transactions, which no snapshot reaches. Stop the agents on every node, delete the
`<service>.position` key from the `failover` bucket, then start the new agents: the first leader serves its local
databases and ships them. The `lease-<service>` buckets and `replica-<service>-<database>` object stores are left
over and can be deleted.

## Tests

```
make test          # unit tests
make integration   # the NATS adapter against a real server (docker)
make stand-test    # failure scenarios on a three-server stand (docker)
make chaos         # random failures in a loop; CHAOS_STEPS, CHAOS_SEED
```

`KEEP_STAND=1` leaves the stand up after a test for a look. The stand (`tests/stand`) runs NATS on three containers
with 60±20 ms between them, the agent on two, and a writer that logs numbered rows. Tests judge by that log and
the final database: no two nodes write at once (but for the
moment after a freeze), nothing committed earlier than the change check (1 s; isolation: 70% of the TTL) before its
leader stopped is lost, a new leader serves within a minute.

CI runs each stand scenario in its own job and a short chaos run with a fixed seed on every pull request and push to
`master`, with the stand images from the Actions cache; the full chaos run is nightly.
