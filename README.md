# Raft KV Store

A distributed, replicated key-value store in Java, built on a **hand-implemented
Raft consensus module** (no consensus libraries — Raft is written from the
paper). Networking uses plain `java.nio` channels; serialization uses Java's
built-in `Serializable`. Zero external dependencies.

Reference: Diego Ongaro & John Ousterhout, *"In Search of an Understandable
Consensus Algorithm"* (the Raft paper). Javadoc on the core methods cites the
specific paper sections they implement.

## Architecture

```
com.raftkv
 ├── Server.java              process entrypoint: wires one node's 3 layers together
 ├── util/                    NodeConfig, ClusterConfig (static membership), Logger
 ├── rpc/                     networking layer (java.nio) — knows nothing about Raft semantics
 │    ├── RequestVoteArgs/Result, AppendEntriesArgs/Result   (paper Figure 2 RPCs)
 │    ├── RpcClient                                          blocking NIO client, one call = one connection
 │    ├── RpcServer                                          NIO accept loop, dispatches to RaftRpcHandler
 │    └── RaftRpcHandler                                     interface RaftNode implements — the seam
 ├── raft/                    the consensus core — no I/O, pure state machine
 │    ├── RaftNode                                           election, replication, commit tracking
 │    ├── RaftState, LogEntry
 ├── kv/                      the replicated application state machine
 │    ├── Command, KVStateMachine
 └── client/                  client-facing surface
      ├── ClientApiServer     TCP text API: PUT/GET/STATUS, leader redirect, NO_LEADER handling
      └── Client               dependency-free client used by CLI + demo/test scripts
```

Each cluster node is its own JVM process listening on two ports: a **Raft
port** (peer-to-peer RequestVote/AppendEntries) and a **client port** (KV
API). Separating them mirrors how real systems like etcd/Consul isolate
control-plane and data-plane traffic.

## What's implemented

- **Leader election** with randomized election timeouts (2500–5000ms) to avoid
  repeated split votes; the election-restriction check (§5.4.1) ensures only a
  candidate with an up-to-date log can win.
- **Pre-Vote** (Ongaro's PhD thesis §4.2.3, not in the original paper but a
  standard production extension used by e.g. etcd): before committing to a
  real, term-incrementing candidacy, a node first runs a non-binding probing
  round. Without this, a node that can never actually win an election (stale
  log after rejoining, or just one that's been briefly out of contact) can
  still repeatedly force every other node's term upward and depose a working
  leader forever — because the core algorithm's term-adoption rule is
  unconditional, independent of whether the vote itself would be granted.
  Pre-Vote closes that: peers evaluate the same log-up-to-date criteria (plus
  "have I heard from a real leader recently") without mutating any state, so
  a doomed candidacy never disrupts anyone.
- **Log replication** via AppendEntries, including the log-consistency check,
  conflicting-suffix truncation, and the extended-paper "fast backtracking"
  optimization (leader skips a whole conflicting term in one round trip
  instead of decrementing `nextIndex` by one per RPC).
- **Heartbeats** — empty AppendEntries sent every 400ms by the leader, with
  per-peer in-flight tracking so a heartbeat is skipped (not queued on top of
  a slow one) if the previous one to that peer hasn't completed — this
  prevents thread/connection pile-up under contention, which in earlier
  testing was found to starve the timer thread responsible for sending
  heartbeats on schedule, causing exactly the kind of election livelock
  Pre-Vote also guards against. All RPC/connection thread pools are bounded
  (fixed-size) rather than unbounded cached pools for the same reason.
  Heartbeats share the exact same code path as real replication.
- **Correct term handling** — stale-term RPCs are rejected; any RPC or
  response carrying a newer term forces an immediate step-down to follower.
- **Commit index / apply loop**, with the §5.4.2 safety rule that a leader
  only commits entries from its own current term directly (implemented via a
  no-op "barrier" entry appended immediately on election).
- **Linearizable reads via the read-index protocol** (paper §8): before
  serving a GET, the leader confirms — via a fresh round-trip to a live
  majority, not just its last-known status — that it is still genuinely the
  leader right now. This is what actually makes reads linearizable, as
  opposed to "usually correct"; see the "Fixed" notes further down for the
  real violation this closed.
- **Client API** (`PUT key value`, `GET key`, `STATUS`) over a simple
  newline-delimited TCP protocol. Non-leader nodes respond `NOT_LEADER
  host:port` so clients can redirect; if no leader is currently known
  (mid-election), nodes respond `NO_LEADER` immediately rather than hanging.
- **Failure handling** — a killed leader is detected by followers via missed
  heartbeats, a new election fires automatically, and a later-rejoining node
  catches up its log via ordinary AppendEntries log reconciliation (no special
  "rejoin" code path needed — this falls straight out of the core algorithm).

**Explicitly out of scope** (per the project spec): log compaction /
snapshotting — the log is kept in memory and grows unbounded. (Linearizable
reads were originally scoped as a stretch goal but have since been
implemented via the read-index protocol — see above.)

## Building & running

Two ways to build this, side by side:

### Option A: Maven (standard Java project layout)

The project is now laid out in Maven's conventional structure
(`src/main/java/...` + `pom.xml`) and has zero dependencies to download other
than Maven's own compiler/jar plugins.

```bash
mvn package
java -jar target/raft-kv-store.jar 1 config/cluster.conf   # starts node 1
java -cp target/raft-kv-store.jar com.raftkv.client.Client config/cluster.conf 0 STATUS
```

Convenience wrappers: `scripts/mvn-build.sh` / `scripts\mvn-build.ps1`.

> **Honest caveat:** this Maven setup was written but **not run/verified** —
> the sandbox this project was built in has no network access to Maven
> Central, so `mvn package` has never actually been executed against this
> code. The `pom.xml` is straightforward (zero dependencies, two very
> standard plugins) and should work, but if it doesn't on the first try,
> Option B below is the one that's been built and re-verified dozens of
> times throughout this project's development.

### Option B: plain `javac` + scripts (no Maven required, fully verified)

Requires JDK 21+. No build tool needed at all — just `javac`, driven by
small scripts.

### macOS / Linux

```bash
scripts/build.sh                 # compiles everything into out/
scripts/start-cluster.sh         # launches the 5 nodes in config/cluster.conf
scripts/stop-cluster.sh          # kills them

# talk to the cluster (0 = try any node, follow redirects automatically)
java -cp out com.raftkv.client.Client config/cluster.conf 0 PUT foo bar
java -cp out com.raftkv.client.Client config/cluster.conf 0 GET foo
java -cp out com.raftkv.client.Client config/cluster.conf 1 STATUS   # node 1's own view
```

### Windows (native PowerShell — no WSL needed)

Equivalent `.ps1` scripts are provided and have been run end-to-end on real
PowerShell 7:

```powershell
scripts\build.ps1
scripts\start-cluster.ps1
scripts\stop-cluster.ps1

java -cp out com.raftkv.client.Client config\cluster.conf 0 PUT foo bar
java -cp out com.raftkv.client.Client config\cluster.conf 0 GET foo
java -cp out com.raftkv.client.Client config\cluster.conf 1 STATUS
```

If scripts refuse to run due to execution policy, run once per session:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Cluster size/ports are configured in `config/cluster.conf` (CSV:
`id,host,raftPort,clientPort`) — add or remove lines to run 3/5/7 nodes.

### Visualizing a chaos test run

`ChaosTestRunner` writes three files per run: `<name>.json` (the full operation
history), `<name>-events.json` (leader kill/restart timestamps), and, if a
violation was found, `<name>-violations.json` (the minimal counterexample).

Open `tools/visualizer.html` directly in a browser (no server needed) and
drag all three files onto it. You get a per-key timeline: each PUT/GET as a
horizontal bar positioned by its real invoke→complete time, colored by
outcome (blue=PUT, green=GET, orange=INDETERMINATE), with dashed red/green
vertical lines marking leader kills/restarts. If a violation was found, its
witness operations are outlined in yellow and hoverable for detail — this is
the fastest way to *see* why a run failed instead of reading raw JSON.

```bash
java -cp out com.raftkv.chaos.ChaosTestRunner config/cluster.conf 25 6 5 demo-history.json
# then open tools/visualizer.html and drop in:
#   demo-history.json, demo-history-events.json, demo-history-violations.json (if present)
```

**Fixed — read-index for linearizable reads:** the original mitigation
(`isReadyToServeReads()`, gating GET on the leader's own current-term barrier
commit) only protected against a *newly elected* leader serving reads before
catching up. It did nothing for the mirror-image case — a leader that has
been process-killed (or, more generally, network-partitioned away from the
cluster) but hasn't yet noticed, since nothing forces it to re-check its own
legitimacy before answering a read. `RaftNode.confirmLeadership()` closes
this properly: before serving any GET, the leader now performs a fresh
round-trip to a majority of the cluster (the standard Raft "read index"
technique, paper section 8) and only proceeds once a live majority has
acknowledged its current term *right now* — not based on a possibly-stale
last-known heartbeat status. This trades a small amount of read latency
(one network round-trip; measured as negligible relative to normal client
overhead) for an actual linearizability guarantee. A related bug was found
alongside this: `ChaosTestRunner`'s process management used plain
`Process.destroyForcibly()`, which — like the PowerShell scripts before their
own fix — could silently fail to kill the real JVM on systems where `java`
resolves through a launcher/redirector (e.g. Oracle's `javapath` on
Windows) that stays alive as a parent process. This was likely inflating the
apparent violation rate by leaving "killed" leaders alive to serve stale
reads for real. Both are now fixed; `ChaosTestRunner` tracks every OS
process matching a node via `ProcessHandle`, cross-platform, the same way
the PowerShell scripts do. Verified via 8 consecutive chaos runs (roughly
5,000-6,500 operations each) plus one longer 14,000-operation run — zero
linearizability violations across all of them, compared to the earlier
~7-out-of-8 pass rate.

**Fixed since initial chaos testing:** an election livelock was found via
real-world testing (not caught in the sandbox) where a node that could never
win an election — e.g. one with a stale/empty log right after rejoining —
could still repeatedly force every live node's term upward and depose a
functioning leader, over and over, because term-adoption in the core
algorithm is unconditional (independent of whether the vote would actually
be granted). Root cause was two-fold: (1) unbounded thread pools for RPC
dispatch could accumulate threads under any contention and starve the timer
thread responsible for sending heartbeats on schedule, and (2) nothing
stopped a doomed candidacy from disrupting the cluster even once heartbeats
were flowing normally. Fixed via bounded thread pools + per-peer in-flight
heartbeat tracking (addresses 1) and the Pre-Vote extension (addresses 2).
Verified via 8+ full demo runs and multiple 20-25s chaos runs (several
thousand concurrent operations each) with zero thrashing — kills now produce
exactly one clean re-election, not runaway term inflation.

### Docker Deployment

`Dockerfile` + `docker-compose.yml` run the same 5-node cluster as isolated
containers on a shared Docker network, using Docker Compose's built-in
service-name DNS instead of hardcoded IPs — `config/cluster-docker.conf`
lists hosts as `raft-node-1` .. `raft-node-5` rather than `127.0.0.1`.
**No application code changes were needed for this**: Java's networking
layer resolves hostnames via DNS natively, so the same `RaftNode`/
`ClientApiServer` code that works with IPs works identically with Docker
service names.

```bash
docker compose up --build
```

This builds one shared image (multi-stage: compiles with a JDK, ships with
a slim JRE) and starts all 5 containers, each running one node, connected
on a private bridge network for node-to-node Raft traffic. Only the
client-API ports (`8001`-`8005`) are published to your host; the Raft ports
(`9001`-`9005`) only need to be reachable inside the Docker network.

**Talking to the cluster from your host machine:** the leader-redirect
mechanism embeds each node's own hostname (e.g. `raft-node-3:8003`) in its
responses, since that's the address other cluster members know it by. For
your host machine to follow those redirects too (not just other
containers), add the same hostnames to your hosts file pointing at
localhost, once:

```
127.0.0.1 raft-node-1 raft-node-2 raft-node-3 raft-node-4 raft-node-5
```

(`/etc/hosts` on macOS/Linux, `C:\Windows\System32\drivers\etc\hosts` as
Administrator on Windows.) After that, use the CLI client exactly as
before, just pointed at the Docker config:

```bash
java -cp out com.raftkv.client.Client config/cluster-docker.conf 0 PUT foo bar
java -cp out com.raftkv.client.Client config/cluster-docker.conf 0 GET foo
```

To stop everything: `docker compose down`.

> **Verification note:** this sandbox's network policy blocks all container
> registries (Docker Hub, GHCR, etc. all return `403`), so the actual
> `docker build` / `docker compose up` has **not** been run end-to-end here
> — only independently verified: (1) the `Dockerfile` parses correctly and
> fails at exactly the expected step (the blocked registry pull, confirmed
> by running `docker build` against it directly); (2) the build stage's
> exact compile command and the runtime stage's exact `ENTRYPOINT` command
> were each manually replicated outside Docker and both work correctly;
> (3) most importantly, the actual new variable Docker introduces —
> hostname-based cluster addressing instead of IPs, including the
> leader-redirect mechanism — was fully tested end-to-end (5-node cluster,
> real PUT/GET/redirect traffic, all via hostnames) and works with zero
> code changes. Running `docker compose up --build` on a machine with
> normal internet access is the remaining step to confirm the full,
> literal container build.

### Full failure-injection demo

```bash
scripts/demo.sh       # macOS/Linux
```
```powershell
scripts\demo.ps1      # Windows
```

This single script: builds, starts a 5-node cluster, waits for election,
performs several `PUT`s, **kills the current leader process**, waits for
automatic re-election, confirms writes still succeed against the new leader,
then **restarts the crashed node** and confirms it catches up to the cluster's
log (matching `commitIndex`/`logSize`). Every phase prints each node's
`STATUS` so the whole run is screenshot-able. Per-node event logs (election
started, vote granted, leader elected, entry committed, entry applied) are
also written to `logs/node<N>.log` with millisecond timestamps.

## Design notes / interview talking points

- **Why records for RPC messages**: Java 21 records give free
  immutability + `Serializable` + structural equality for the Figure-2 RPC
  argument/result types, cutting a lot of boilerplate versus hand-written POJOs.
- **Why one connection per RPC** instead of persistent peer connections: at
  Raft's message rate (a handful of RPCs per heartbeat interval per peer),
  connection setup cost is negligible, and it sidesteps a whole class of
  connection-lifecycle bugs (stale connections after a peer restarts, etc.) —
  a deliberate simplicity-over-throughput tradeoff, called out explicitly
  rather than left implicit.
- **Locking model**: all Raft state mutation goes through a single
  `ReentrantLock` per node; RPCs are always sent *outside* the lock so a slow
  or dead peer can never block the node's own progress. This is what lets
  heartbeats and elections stay responsive even while other RPCs are in flight.
- **No-op barrier entry on election**: directly implements the subtle §5.4.2
  safety case — without it, a new leader could apply an old-term entry to a
  majority without a safe way to advance `commitIndex` past it.
