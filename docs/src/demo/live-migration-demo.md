# Live Migration demo with CCM

A copy-paste walkthrough of the Sidecar **Live Migration** feature (CEP-40),
run entirely on one laptop using [`ccm`](https://github.com/apache/cassandra-ccm).

We migrate **node1 -> node4**:

```
   node1 (127.0.0.1)  -- copy SSTables over HTTP -->  node4 (127.0.0.4)
   (source, running)                                 (destination, stopped)
```

The destination comes up assuming the source's identity (same `host_id` and
tokens) because the **entire** on-disk data set -- including the Cassandra
`system` keyspace -- is copied. That is why Phase 5 just starts Cassandra
normally: no `replace_address`, no re-bootstrap.

**One Sidecar, both roles.** Everything here runs on one laptop, so we run a
single Sidecar process that manages *all* the cluster's Cassandra instances
(node1-node4). For the migration it acts as the **source** for requests
addressed to node1's IP and the **destination** for node4's IP -- the role is
decided per request from the Host header, via `migration_map`. (In production
the source and destination are separate hosts, each running its own Sidecar,
talking to each other over the network; on one machine a single Sidecar is the
simplest way to try the feature.)

> **Demo only.** Access control and TLS are turned off so the `curl` calls stay
> readable. Do not model a production deployment on the `sidecar.yaml` below.

The full reference is `docs/src/live-migration.adoc`; this is the hands-on version.

---

## 0. Prerequisites

- `ccm`, a JDK, `curl`, and the Sidecar repo checked out.
- On **macOS**, the loopback aliases `127.0.0.2`-`127.0.0.4` must exist
  (Linux routes all of `127.0.0.0/8` already):

```bash
for n in 2 3 4; do sudo ifconfig lo0 alias 127.0.0.$n up; done
```

- **macOS only -- free up port 7000.** CCM's default `storage_port` is `7000`,
  which the macOS **AirPlay Receiver** (`ControlCenter`) also listens on. If
  it's enabled, `ccm start` fails with
  `Inet address 127.0.0.1:7000 is not available: Operation not permitted`.
  Turn it off in **System Settings -> General -> AirDrop & Handoff -> AirPlay
  Receiver = Off**, then confirm nothing holds the port:

```bash
lsof -nP -iTCP:7000 -sTCP:LISTEN     # should print nothing once AirPlay is off
```

Shell variables used throughout:

```bash
CLUSTER=lmdemo
SRC=127.0.0.1   # node1, the source
DST=127.0.0.4   # node4, the destination
SRC_API=http://$SRC:9043/api/v1/live-migration
DST_API=http://$DST:9043/api/v1/live-migration
```

---

## 1. Create the source cluster and load data

```bash
ccm create $CLUSTER -v 4.1.3 -n 3
ccm start
ccm node1 nodetool status        # three nodes, all UN

# Save node1's host_id; Phase 5 confirms node4 takes it over.
NODE1_HOSTID=$(ccm node1 nodetool info | awk '/^ID/ {print $NF}')
echo "node1 host_id: $NODE1_HOSTID"

ccm node1 cqlsh -e "
CREATE KEYSPACE demo_ks WITH replication = {'class':'SimpleStrategy','replication_factor':3};
CREATE TABLE demo_ks.users (id int PRIMARY KEY, name text);
INSERT INTO demo_ks.users (id,name) VALUES (1,'ada');
INSERT INTO demo_ks.users (id,name) VALUES (2,'grace');
INSERT INTO demo_ks.users (id,name) VALUES (3,'linus');
INSERT INTO demo_ks.users (id,name) VALUES (4,'dennis');"

# Bulk-load the same table (flush per batch) so the copy has several SSTables to move.
for batch in $(seq 1 5); do
  ccm node1 cqlsh -e "$(for i in $(seq $((batch*1000)) $((batch*1000+999))); do
    echo "INSERT INTO demo_ks.users (id,name) VALUES ($i,'u$i');"; done)"
  ccm node1 nodetool flush     # each batch -> its own SSTable
done

ccm flush                        # flush every node so there are real files to copy

# Baseline: read at LOCAL_ONE with TRACING ON. The trace's "Read-repair" /
# "Reading data from /..." lines should point at /127.0.0.1 (node1 itself
# answers the read). Phase 5 reruns the same command on node4 and we expect
# the same row but with /127.0.0.4 as the source instead.
ccm node1 cqlsh -e "CONSISTENCY LOCAL_ONE; TRACING ON; SELECT * FROM demo_ks.users WHERE id = 1;"
```

CCM lays each node out under `~/.ccm/lmdemo/nodeN/`:

| What | Path (node1) |
|------|--------------|
| data dir | `~/.ccm/lmdemo/node1/data0` |
| commitlog | `~/.ccm/lmdemo/node1/commitlogs` |
| hints | `~/.ccm/lmdemo/node1/hints` |
| saved caches | `~/.ccm/lmdemo/node1/saved_caches` |
| cassandra.yaml | `~/.ccm/lmdemo/node1/conf/cassandra.yaml` |

IP/port scheme: `node1=127.0.0.1 jmx 7100`, `node2=127.0.0.2 jmx 7200`,
`node3=127.0.0.3 jmx 7300`, native port `9042` on every node.

---

## 2. Add the destination node -- but do **not** start it

Live Migration refuses to copy onto a running Cassandra (it would corrupt the
SSTable directory). So we add the node and leave it stopped.

```bash
ccm add node4 -i 127.0.0.4 -j 7400   # no -b (no bootstrap), and don't start it
```

This creates `~/.ccm/lmdemo/node4/` (jmx 7400, native `127.0.0.4:9042`) with
empty data dirs, ready to receive the copy. (If you created the cluster with
`--vnodes`, `ccm add` will require a datacenter -- append `-d datacenter1`, the
name `SimpleSnitch` reports in `nodetool status`.)

---

## 3. Configure and run the Sidecar

One Sidecar process manages all four Cassandra instances. It binds
`0.0.0.0:9043` so that every node IP reaches the same process -- in particular
`127.0.0.1:9043` (source / node1) and `127.0.0.4:9043` (destination / node4);
the Host header on each request selects the role. We launch it straight from
the source tree with `./gradlew run`, pointing it at a demo config file.

### Sidecar config

Create a demo config file -- keep it **outside** the repo so a `./gradlew clean`
can't wipe it, e.g. `~/lm-demo/sidecar.yaml` (run `mkdir -p ~/lm-demo` first).
The Sidecar expands a leading `~` to your home directory, so the Cassandra
paths below work as-is. List **all four** CCM nodes under `cassandra_instances`
-- this one Sidecar fronts the whole local cluster -- and set `sidecar.host` to
`0.0.0.0`. Only node1 and node4 appear in `migration_map`, so they're the only
ones with a live-migration role; node2/node3 are managed but return `404` on
the live-migration endpoints (neither source nor destination):

```yaml
cassandra_instances:
  - id: 1                       # SOURCE -> node1
    host: 127.0.0.1
    port: 9042
    storage_dir: ~/.ccm/lmdemo/node1
    data_dirs: [ ~/.ccm/lmdemo/node1/data0 ]
    staging_dir: ~/.ccm/lmdemo/node1/sstable-staging
    commitlog_dir: ~/.ccm/lmdemo/node1/commitlogs
    hints_dir: ~/.ccm/lmdemo/node1/hints
    saved_caches_dir: ~/.ccm/lmdemo/node1/saved_caches
    jmx_host: 127.0.0.1
    jmx_port: 7100
    jmx_ssl_enabled: false
  - id: 2                       # node2 (not part of the migration)
    host: 127.0.0.2
    port: 9042
    storage_dir: ~/.ccm/lmdemo/node2
    data_dirs: [ ~/.ccm/lmdemo/node2/data0 ]
    staging_dir: ~/.ccm/lmdemo/node2/sstable-staging
    commitlog_dir: ~/.ccm/lmdemo/node2/commitlogs
    hints_dir: ~/.ccm/lmdemo/node2/hints
    saved_caches_dir: ~/.ccm/lmdemo/node2/saved_caches
    jmx_host: 127.0.0.2
    jmx_port: 7200
    jmx_ssl_enabled: false
  - id: 3                       # node3 (not part of the migration)
    host: 127.0.0.3
    port: 9042
    storage_dir: ~/.ccm/lmdemo/node3
    data_dirs: [ ~/.ccm/lmdemo/node3/data0 ]
    staging_dir: ~/.ccm/lmdemo/node3/sstable-staging
    commitlog_dir: ~/.ccm/lmdemo/node3/commitlogs
    hints_dir: ~/.ccm/lmdemo/node3/hints
    saved_caches_dir: ~/.ccm/lmdemo/node3/saved_caches
    jmx_host: 127.0.0.3
    jmx_port: 7300
    jmx_ssl_enabled: false
  - id: 4                       # DESTINATION -> node4
    host: 127.0.0.4
    port: 9042
    storage_dir: ~/.ccm/lmdemo/node4
    data_dirs: [ ~/.ccm/lmdemo/node4/data0 ]
    staging_dir: ~/.ccm/lmdemo/node4/sstable-staging
    commitlog_dir: ~/.ccm/lmdemo/node4/commitlogs
    hints_dir: ~/.ccm/lmdemo/node4/hints
    saved_caches_dir: ~/.ccm/lmdemo/node4/saved_caches
    jmx_host: 127.0.0.4
    jmx_port: 7400
    jmx_ssl_enabled: false

sidecar:
  host: 0.0.0.0                 # bind all loopback IPs, so every node IP reaches this process
  port: 9043
  schema:
    is_enabled: false
  coordination:
    cluster_lease_claim:
      enabled: false

access_control:
  enabled: false
  authorizer:
    - class_name: org.apache.cassandra.sidecar.acl.authorization.AllowAllAuthorizationProvider

driver_parameters:
  # Use nodes that survive the migration as contact points -- node1 (the
  # source) is stopped in Phase 2, so don't rely on it here.
  contact_points: [ "127.0.0.2:9042", "127.0.0.3:9042" ]
  # CQL driver credentials the Sidecar uses to talk *to* Cassandra. Cassandra
  # still has its own auth; this is separate from access_control above, which
  # gates the Sidecar's own HTTP API.
  username: cassandra
  password: cassandra

cluster_topology_monitor: { enabled: false }
sidecar_peer_health:      { enabled: false }

live_migration:
  files_to_exclude: [ ]
  dirs_to_exclude:
    - glob:${DATA_FILE_DIR}/*/*/snapshots
  migration_map:
    127.0.0.1: 127.0.0.4        # source -> destination
  max_concurrent_file_requests: 20
```

> How the role is chosen: the Sidecar reads each request's Host header. A
> request to `127.0.0.1:9043` matches a **key** in `migration_map` -> source; a
> request to `127.0.0.4:9043` matches a **value** -> destination. When the
> destination pulls files it connects to the source's IP on its own port
> (`127.0.0.1:9043`), which is handled by this same process. A host
> not in the map gets `404` on the live-migration endpoints -- that disables
> the API for that host.

### Launch it

In a **separate terminal** (the Sidecar runs in the foreground and streams its
logs there), from the repo root:

```bash
cd <sidecar-repo>
./gradlew run -x test -Dsidecar.config=file://$HOME/lm-demo/sidecar.yaml
```

`-Dsidecar.config` takes a `file://` URI to an **absolute** path; the `run`
task forwards it to the Sidecar JVM. Leave this running and use your original
terminal for the `curl` / `ccm` commands below.

Confirm it's up and answers for both roles:

```bash
curl -sS $SRC_API/status     # 200, NOT_COMPLETED  (source / node1)
curl -sS $DST_API/status     # 200, NOT_COMPLETED  (destination / node4)
```

> The Sidecar runs even though node4's Cassandra is down -- that is the whole
> point. You'll see CQL/JMX connection retries for node4 in the log; ignore them
> until Phase 5.

---

## Phase 1 -- Initial copy (source still serving)

Submit the copy **on the destination**. `successThreshold < 1.0` lets the
iteration pass while the source is still taking writes.

```bash
curl -sS -X POST $DST_API/data-copy-tasks \
  -H 'Content-Type: application/json' \
  -d '{"maxIterations":3,"successThreshold":0.95,"maxConcurrency":4}'
# -> 202 {"taskId":"...","statusUrl":"/api/v1/live-migration/data-copy-tasks/..."}
```

Poll until the last `status[]` entry is `SUCCESS` (or `FAILED`/`CANCELLED`):

```bash
curl -sS http://$DST:9043/api/v1/live-migration/data-copy-tasks/<taskId> | python3 -m json.tool
```

---

## Phase 2 -- Stop the source, final sync at `1.0`

```bash
ccm node1 nodetool drain
ccm node1 stop

curl -sS -X POST $DST_API/data-copy-tasks \
  -H 'Content-Type: application/json' \
  -d '{"maxIterations":3,"successThreshold":1.0,"maxConcurrency":4}'
```

Most data was copied in Phase 1, so this is quick. Poll for `SUCCESS` as above.

---

## Phase 3 -- Verify byte-for-byte

```bash
curl -sS -X POST $DST_API/files-verification-tasks \
  -H 'Content-Type: application/json' \
  -d '{"maxConcurrency":4,"digestAlgorithm":"MD5"}'

curl -sS http://$DST:9043/api/v1/live-migration/files-verification-tasks/<taskId> | python3 -m json.tool
```

Success = `state: COMPLETED` **and** every mismatch counter
(`filesNotFoundAtSource`, `filesNotFoundAtDestination`, `metadataMismatches`,
`digestMismatches`, `digestVerificationFailures`) is `0`.

---

## Phase 4 -- Mark migration complete on **both** instances

Completion is recorded per instance (each writes its own
`live_migration_status.json` under its staging dir), so call `/status` for
both -- the Host header selects which instance. node1's Cassandra is down
after Phase 2, but the Sidecar still serves its API and just records completion
to disk:

```bash
curl -sS -X POST $SRC_API/status     # -> {"state":"COMPLETED","endTime":...}
curl -sS -X POST $DST_API/status
```

After this, new data-copy/verification tasks on these hosts return `400`.

---

## Phase 5 -- Start the destination Cassandra

### Fix the seed lists first

`ccm create -n 3` marks **all three nodes as seeds**, so the source's address
(`127.0.0.1`) is in the `seeds` list of *every* node's `cassandra.yaml`,
including node4's. node1 is now gone, and node4 has taken over its identity --
so replace `127.0.0.1` with node4's address (`127.0.0.4`) in the seed list of
the surviving/affected nodes:

```bash
for n in node2 node3 node4; do
  yaml=~/.ccm/lmdemo/$n/conf/cassandra.yaml
  # only touch the seeds line; leave listen_address/rpc_address alone
  sed -i.bak -E '/seeds:/ s/127\.0\.0\.1/127.0.0.4/g' "$yaml" && rm -f "$yaml.bak"
done
```

> The edits to the *running* node2/node3 only take effect on their next
> restart. For this demo they keep working regardless, because node2 and node3
> are themselves seeds and seed each other; the stale entry just shouldn't be
> left behind. (In production, do a rolling restart of node2/node3 so the new
> seed list is picked up.)

### Start node4

Start it **normally** -- it reads the copied `system` keyspace and comes up as
the former node1 (same `host_id` and tokens) at its new IP:

```bash
ccm node4 start
ccm node4 nodetool status                       # expect 127.0.0.4  UN
```

### Verify node4 is serving its own copy

With `RF=3` and three surviving nodes, every node holds every row -- so a
`SELECT *` against node4 isn't proof on its own (node2/node3 can satisfy the
read as replicas). These four checks together prove (a) the SSTables copied
in Phases 1-2 are loaded locally on node4, (b) node4 inherited node1's token
ranges, (c) node4 took over node1's `host_id` rather than bootstrapping fresh,
and (d) reads are actually answered by node4 itself.

```bash
# 1. Local SSTables exist and were loaded -- non-zero count and live disk usage.
ccm node4 nodetool tablestats demo_ks.users | grep -E "SSTable count|Space used \(live\)"

# 2. node4 inherited node1's token ranges -- for each partition key, node4 is
#    in the replica set and 127.0.0.1 is gone. This is a topology check: it
#    proves Cassandra will *route* reads for these keys to node4. (Whether
#    node4 actually has the bytes on disk is what #1 and #4 cover.) With RF=3
#    and three surviving nodes, every node is a replica for every key, so
#    expect 127.0.0.2 / .3 / .4 for each.
for key in 1 2 3 4; do ccm node4 nodetool getendpoints demo_ks users $key; done | sort -u

# 3. host_id was adopted, not freshly generated -- should match $NODE1_HOSTID
#    captured in Phase 1.
NODE4_HOSTID=$(ccm node4 nodetool info | awk '/^ID/ {print $NF}')
[ "$NODE1_HOSTID" = "$NODE4_HOSTID" ] \
  && echo "host_id matches: $NODE4_HOSTID" \
  || echo "MISMATCH: node1=$NODE1_HOSTID node4=$NODE4_HOSTID"

# 4. The read is served by node4. Same command as the Phase 1 baseline -- the
#    trace should now show /127.0.0.4 where it previously showed /127.0.0.1,
#    and no READ messages dispatched to .2 / .3.
ccm node4 cqlsh -e "CONSISTENCY LOCAL_ONE; TRACING ON; SELECT * FROM demo_ks.users WHERE id = 1;"
```

### Confirm the cluster is healthy (check the logs)

Verify node4 joined cleanly and the surviving nodes accepted the IP change.
The whole ring should be `UN` (Up/Normal) and the logs should be free of
gossip/startup errors:

```bash
# 1. Every node Up/Normal, and node1's old IP (127.0.0.1) is gone.
ccm node2 nodetool status
ccm node2 nodetool gossipinfo | grep -E "/127.0.0.4|STATUS|LOAD" | head

# 2. node4 reports a normal startup (not a bootstrap/replacement).
ccm node4 showlog | grep -iE "Startup complete|Node .* state jump to NORMAL|now part of the cluster" | tail

# 3. Scan all live nodes for errors/warnings raised since the migration.
for n in node2 node3 node4; do
  echo "== $n =="
  ccm $n showlog 2>/dev/null | grep -iE "ERROR|gossip.*(mismatch|conflict)|Unable to|Exception" | tail -5
done
```

What "healthy" looks like:

- `nodetool status` lists **three** `UN` nodes -- `127.0.0.2`, `127.0.0.3`, and
  `127.0.0.4` -- and **not** `127.0.0.1` (its `host_id` now lives at `.4`).
- node4's log shows `Startup complete` / `state jump to NORMAL` with **no**
  `Bootstrap` or `replace` activity (it adopted the copied identity).
- No `ERROR`s and no gossip "host_id/endpoint mismatch" or
  "Unable to gossip with any peers" lines on the surviving nodes.

> Don't be alarmed by a little gossip churn around the old address as node4
> takes over node1's identity -- e.g. node4 logging
> `Node /127.0.0.1:... is now part of the cluster`, or node2/node3 briefly
> showing node1 as `DOWN`. It's expected and settles on its own; what matters
> is that `nodetool status` ends with `127.0.0.4` `UN`.

---

## Phase 6 -- Turn the API back off (clear map + `DELETE /status`)

Edit `~/lm-demo/sidecar.yaml` and clear the map:

```yaml
live_migration:
  migration_map: { }    # was: 127.0.0.1: 127.0.0.4
  files_to_exclude: [ ]
  dirs_to_exclude:
    - glob:${DATA_FILE_DIR}/*/*/snapshots
  max_concurrent_file_requests: 20
```

Restart the Sidecar (`Ctrl-C` in its terminal, then re-run the same
`./gradlew run ...` command), confirm the API is now off, then clear the
completion records (`DELETE` requires the host to already be out of the map):

```bash
curl -sS -o /dev/null -w '%{http_code}\n' $DST_API/data-copy-tasks   # -> 404 (API disabled)
curl -sS -X DELETE $DST_API/status
curl -sS -X DELETE $SRC_API/status
```

Live Migration is now complete.

Finally, node1 has been fully replaced by node4 -- clean up the leftover node1
info:

```bash
ccm node1 remove        # drop the replaced source node from ccm
```

If `127.0.0.1` still lingers in `nodetool status`, it's harmless and ages out
on its own -- or evict it now with `ccm node4 nodetool assassinate 127.0.0.1`.
You can also drop node1's entry from `cassandra_instances` in
`~/lm-demo/sidecar.yaml`.

---

## Cleanup

```bash
# stop the Sidecar with Ctrl-C in its terminal (or: pkill -f CassandraSidecarDaemon)
ccm stop lmdemo                      # stop all Cassandra daemons
ccm remove lmdemo                    # drop the cluster
rm -rf ~/lm-demo                     # the demo config
# optional (macOS): drop the loopback aliases created in step 0
# for n in 2 3 4; do sudo ifconfig lo0 -alias 127.0.0.$n; done
# (leave them in place if you plan to re-run the demo)
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `404` on a live-migration endpoint | Host isn't in `migration_map`, or you hit the wrong IP. The source IP must be a **key** and the destination IP a **value** in the map. |
| `400 "Cassandra is currently running"` on `POST /data-copy-tasks` | node4 is up. It must be stopped during the copy (`ccm node4 stop`). |
| `409 Conflict` | A task is already running on that instance -- wait, or `PATCH .../<taskId>` to cancel. |
| `curl` to `127.0.0.4:9043` connection-refused | Sidecar isn't bound to all IPs -- set `sidecar.host: 0.0.0.0`. The `127.0.0.4` loopback alias must also exist (step 0). |
| node4 won't gossip after start | Its seed still points at the now-dead source -- re-check the Phase 5 `sed`. |
| Verification shows mismatches | Re-run a copy at `successThreshold: 1.0`, then verify again. |
