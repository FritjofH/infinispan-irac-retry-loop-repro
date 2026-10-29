# Infinispan IRAC retry loop on evicted keys (16.2+)

Reported upstream: https://github.com/infinispan/infinispan/issues/18383

Reproducer for a busy loop in `DefaultIracManager` (cross-site async backup, IRAC) on a
cache **without a persistence store** that evicts entries.

## The suspected bug

[#16797](https://github.com/infinispan/infinispan/issues/16797) /
[PR #16798](https://github.com/infinispan/infinispan/pull/16798) stopped IRAC discarding a queued key
whose entry and tombstone are both missing, because with passivation the entry may still be on its way
to the store. [#16807](https://github.com/infinispan/infinispan/issues/16807) /
[PR #16808](https://github.com/infinispan/infinispan/pull/16808) then added an immediate
`iracExecutor.run()` after the retry.

`DefaultIracManager.java` in 16.3.1, `sendUpdateBatch()`, lines 426–434:

```java
if (data.entry == null && data.tombstone == null) {
   // Entry may not be loadable yet if passivation is still in progress after eviction.
   // Retry instead of discarding so the next batch cycle can find the entry in the store.
   ...
   data.state.retry();
   iracExecutor.run();
   continue;
}
```

and the `fetchEntry()` exceptionally handler, lines 390–395, which now also does `iracExecutor.run()` after `state.retry()`.

In 16.1.4 the same branch adds the key to `invalidState`, and the key is discarded.

Without a store, an evicted key can never be loaded again (an expired key should behave the same; not tested here). The key is not sent, so it never
completes; this path does not back off; and `IracExecutor` chains another pass at once. IRAC then
runs `DefaultIracManager.run()` again and again. Nothing in this path ends the loop (observed here for 30 s).

Checked in the release source jars: 16.0.14, 16.1.0 and 16.1.4 do not have the
retry + run; 16.2.0 and 16.3.1 do.

## What the reproducer does

`IracRetryLoopRepro` starts two embedded cache managers in one JVM, sites `A` and `B`, joined by
JGroups RELAY2. Everything binds to 127.0.0.1 (ports 7800, 7820, 7840, 7860). The stock `tcp` stack is
used with `TCPPING` in place of `MPING`.

Cache `repro` on site A: `DIST_SYNC`, one node, no store, `memory max-count=10 when-full=REMOVE`,
`ASYNC` backup to B.

1. Site B is **not started**. 50 keys are written to site A. IRAC cannot send them, so all 50 stay
   queued, and 40 of them are evicted. Watched for 5 s.
2. Site B is started and joins the bridge. Watched for 30 s.

Site B is simply absent in step 1 (no `take-offline`, no fault injection). That is the simplest way to
keep keys queued until after they are evicted, and it does not depend on timing.

Measured every second, on site A:

- **pending IRAC keys**: size of `DefaultIracManager.updatedKeys` (read by reflection).
- **IRAC passes/s**: the reproducer wraps the IRAC executor (`IracExecutor.setExecutor`) and counts
  tasks. One task = one `DefaultIracManager.run()` pass.
- **process CPU**: `OperatingSystemMXBean.getProcessCpuTime()` delta / wall time, in cores. The process
  is otherwise idle.

The summary and the verdict use the last 10 s of each phase. IRAC's own exponential back-off
(500 ms initial, doubling, ±50 % random) from the failed sends in step 1 must expire before the loop
starts, so the start time of the loop in step 2 varies by a few seconds.

## Run

JDK 17+ and Maven.

```
mvn -q -Dinfinispan.version=16.3.1 compile exec:java
mvn -q -Dinfinispan.version=16.1.4 compile exec:java
```

Options: `-Dkeys=50 -Ddown.seconds=5 -Dup.seconds=30`.

## Observed

JDK 21.0.11 (Homebrew OpenJDK), macOS arm64, 10 CPUs. Three runs per version; all three gave the same verdict.

Run 1, 16.3.1 (per-second lines trimmed):

```
Infinispan 16.3.1, JDK 21.0.11, 10 CPUs
Wrote 50 keys. Entries in memory: 10. Keys queued for IRAC: 50
--- site B down: measuring for 5 s ---
  t= 1s  pending IRAC keys= 50  IRAC passes/s=       5  process CPU=0.07 cores
  t= 2s  pending IRAC keys= 50  IRAC passes/s=       3  process CPU=0.02 cores
  t= 3s  pending IRAC keys= 50  IRAC passes/s=       0  process CPU=0.01 cores
  t= 4s  pending IRAC keys= 50  IRAC passes/s=       3  process CPU=0.01 cores
  t= 5s  pending IRAC keys= 50  IRAC passes/s=       0  process CPU=0.01 cores
--- site B up: measuring for 30 s ---
  t= 1s  pending IRAC keys= 40  IRAC passes/s= 110,184  process CPU=2.12 cores
  t= 2s  pending IRAC keys= 40  IRAC passes/s= 134,288  process CPU=1.57 cores
  t= 3s  pending IRAC keys= 40  IRAC passes/s= 136,797  process CPU=1.45 cores
  t= 4s  pending IRAC keys= 40  IRAC passes/s= 135,686  process CPU=1.46 cores
  t= 5s  pending IRAC keys= 40  IRAC passes/s= 128,929  process CPU=1.39 cores
  t=30s  pending IRAC keys= 40  IRAC passes/s= 112,726  process CPU=1.20 cores
site B down: pending at end=50  last 5 s: IRAC passes/s=2  process CPU=0.02 cores
site B up:   pending at end=40  last 10 s: IRAC passes/s=126,988  process CPU=1.39 cores
VERDICT Infinispan 16.3.1: RETRY LOOP - evicted keys never drain and IRAC re-runs with nothing to send (40 keys still queued, 126,988 IRAC passes/s, process CPU 1.39 cores)
```

Run 1, 16.1.4 (per-second lines trimmed):

```
Infinispan 16.1.4, JDK 21.0.11, 10 CPUs
Wrote 50 keys. Entries in memory: 10. Keys queued for IRAC: 50
--- site B down: measuring for 5 s ---
  t= 1s  pending IRAC keys= 10  IRAC passes/s=       4  process CPU=0.09 cores
  t= 2s  pending IRAC keys= 10  IRAC passes/s=       2  process CPU=0.03 cores
  t= 3s  pending IRAC keys= 10  IRAC passes/s=       2  process CPU=0.02 cores
--- site B up: measuring for 30 s ---
  t= 1s  pending IRAC keys=  0  IRAC passes/s=       0  process CPU=0.01 cores
  t= 2s  pending IRAC keys=  0  IRAC passes/s=       0  process CPU=0.01 cores
  t= 3s  pending IRAC keys=  0  IRAC passes/s=       0  process CPU=0.01 cores
  t=30s  pending IRAC keys=  0  IRAC passes/s=       0  process CPU=0.00 cores
site B down: pending at end=10  last 5 s: IRAC passes/s=2  process CPU=0.03 cores
site B up:   pending at end=0  last 10 s: IRAC passes/s=0  process CPU=0.00 cores
VERDICT Infinispan 16.1.4: NO LOOP - IRAC queue drained after site B came up (0 keys still queued, 0 IRAC passes/s, process CPU 0.00 cores)
```

Steady state (last 10 s with site B up), runs 1–3:

| Version | Keys still queued | IRAC passes/s | Process CPU (cores) |
|---|---|---|---|
| 16.3.1 run 1 | 40 | 126,988 | 1.39 |
| 16.3.1 run 2 | 40 | 129,877 | 1.40 |
| 16.3.1 run 3 | 40 | 140,029 | 1.43 |
| 16.1.4 run 1 | 0 | 0 | 0.00 |
| 16.1.4 run 2 | 0 | 0 | 0.00 |
| 16.1.4 run 3 | 0 | 0 | 0.00 |

Also run once per version on JDK 25.0.4.1 (Homebrew OpenJDK), same machine:

```
VERDICT Infinispan 16.3.1: RETRY LOOP - evicted keys never drain and IRAC re-runs with nothing to send (40 keys still queued, 114,016 IRAC passes/s, process CPU 1.51 cores)
VERDICT Infinispan 16.1.4: NO LOOP - IRAC queue drained after site B came up (0 keys still queued, 0 IRAC passes/s, process CPU 0.01 cores)
```

In 16.1.4 the 40 evicted keys are discarded on the first IRAC pass (pending drops 50 → 10 while site B is still down); the other 10 are sent when site B comes up. In 16.3.1 all 50 stay queued while site B is down (sends fail and IRAC backs off, so there is no loop yet); when site B is up the 10 live keys are sent, and the 40 evicted keys keep IRAC running with nothing to send.

On JDK 21 the embedded blocking executor is a thread-per-task virtual-thread executor (`ThreadPerTaskExecutor` / `VirtualThread.run` → `IracExecutor$WrappedRunnable.apply` → `DefaultIracManager.run(DefaultIracManager.java:362)` in a logged stack trace), so each IRAC pass is a new virtual thread.

## Seen in production

Infinispan Server 16.3.1 (`quay.io/infinispan/server:16.3.1`, JDK 25, virtual threads enabled,
2 CPUs per pod), two sites, ASYNC backups, caches without a store, using `lifespan` and per-entry TTLs.
The problem started after an upgrade from 16.0.14 and a cross-site connectivity loss.

- One pod at a time ran at ~100 % CPU. Restarting it moved the load to another pod.
- A thread dump showed both `ForkJoinPool` carrier threads busy. The mounted virtual thread was in
  `DefaultIracManager.run(DefaultIracManager.java:362)` → `FlowableConcatMapMaybe.drain`. The virtual
  thread id was ~710 million after ~8,200 s uptime.
- With `org.infinispan.xsite.irac` at TRACE for 2 s, `server.log` got 85,993 lines of
  `[IRAC] Entry and tombstone both null for key …, scheduling retry`, all for **one** key: a short
  String key with a per-entry TTL, in a cache without a store, all with the same `requestId`.
- With the carrier threads busy, the node missed JGroups failure detection. Members were suspected
  and dropped from the view without restarting, and liveness probes timed out.
- Clearing the cache removed the key from the IRAC queue (`trackClear` discards `updatedKeys`).

In production the key was not evicted (`evictions` was 0). The likely path is TTL expiry plus a
change of primary owner during restarts, which leaves the new primary with neither entry nor
tombstone. This is inferred, not reproduced here. Either way, "entry and tombstone both null" is
not always a passivation race, and IRAC retries such a key forever.

## Suggested fix

Do not retry forever a key that cannot ever be loaded. For example:

- Discard the key (the 16.1 behaviour) when the cache has no store / no passivation, because then
  "entry and tombstone both null" cannot be a passivation race.
- Or keep the retry, but do not call `iracExecutor.run()` at once: schedule the re-run with a delay or
  back-off, and give up after a bounded number of attempts.
