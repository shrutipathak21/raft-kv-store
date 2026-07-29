package com.raftkv.chaos;

import com.raftkv.util.ClusterConfig;
import com.raftkv.util.NodeConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ChaosTestRunner {

    public record ChaosEvent(String type, int nodeId, long atMs) {
    }

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        String confPath = args.length > 0 ? args[0] : "config/cluster.conf";
        int durationSec = args.length > 1 ? Integer.parseInt(args[1]) : 25;
        int numWorkers = args.length > 2 ? Integer.parseInt(args[2]) : 6;
        int numKeys = args.length > 3 ? Integer.parseInt(args[3]) : 5;
        String historyPath = args.length > 4 ? args[4] : "chaos-history.json";

        ClusterConfig cluster = ClusterConfig.load(confPath);
        new File("chaos-logs").mkdirs();

        Map<Integer, List<ProcessHandle>> processHandles = new ConcurrentHashMap<>();
        for (NodeConfig n : cluster.allNodes()) {
            processHandles.put(n.id(), startNodeTracked(n.id(), confPath));
        }
        log("Started " + cluster.size() + " node processes. Waiting for initial election...");
        if (!waitForLeader(cluster, 20)) {
            log("WARNING: no leader observed within 20s — continuing anyway, load will likely stall.");
        } else {
            log("Initial leader confirmed.");
        }

        List<Operation> history = Collections.synchronizedList(new ArrayList<>());
        List<ChaosEvent> chaosEventLog = Collections.synchronizedList(new ArrayList<>());
        AtomicLong idGen = new AtomicLong(1);
        AtomicBoolean running = new AtomicBoolean(true);
        long testStartNanos = System.nanoTime();

        String[] keys = new String[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = "k" + i;
        }

        List<Thread> workerThreads = new ArrayList<>();
        for (int i = 1; i <= numWorkers; i++) {
            int processId = i;
            RawClient client = new RawClient(cluster);
            Thread t = new Thread(() -> workerLoop(processId, client, keys, history, idGen, running, testStartNanos),
                    "worker-" + processId);
            t.start();
            workerThreads.add(t);
        }

        AtomicInteger chaosEvents = new AtomicInteger(0);
        Thread chaosThread = new Thread(
                () -> chaosLoop(cluster, processHandles, confPath, running, chaosEvents, chaosEventLog, testStartNanos),
                "chaos-injector");
        chaosThread.start();

        log("Running concurrent load (" + numWorkers + " workers, " + numKeys + " keys) with chaos injection for "
                + durationSec + "s ...");
        Thread.sleep(durationSec * 1000L);
        running.set(false);

        for (Thread t : workerThreads) {
            t.join(4000);
        }
        chaosThread.join(6000);

        log("Stopping remaining node processes...");
        for (List<ProcessHandle> handles : processHandles.values()) {
            for (ProcessHandle h : handles) {
                h.destroyForcibly();
            }
        }

        log("Collected " + history.size() + " operations across " + chaosEvents.get() + " chaos (leader-kill) events.");
        writeHistoryJson(history, historyPath);
        log("Full operation history written to " + historyPath);

        String eventsPath = historyPath.replaceFirst("\\.json$", "") + "-events.json";
        writeEventsJson(chaosEventLog, eventsPath);
        log("Chaos event timeline written to " + eventsPath);

        log("Running linearizability check...");
        LinearizabilityChecker checker = new LinearizabilityChecker();
        List<LinearizabilityChecker.KeyResult> results = checker.check(history);

        log("=== Linearizability Report ===");
        boolean allOk = true;
        List<Operation> allWitnesses = new ArrayList<>();
        for (LinearizabilityChecker.KeyResult r : results) {
            log((r.linearizable() ? "PASS" : "FAIL") + "  key=" + r.key()
                    + "  (" + r.opCount() + " ops)  " + r.explanation());
            if (!r.linearizable()) {
                allOk = false;
                List<Operation> opsForKey = history.stream()
                        .filter(op -> op.key().equals(r.key()) && op.outcome() != Operation.Outcome.FAIL)
                        .toList();
                List<Operation> witness = checker.minimizeFailingWitness(opsForKey);
                allWitnesses.addAll(witness);
                log("  Minimal counterexample for key=" + r.key() + " (" + witness.size() + " ops):");
                for (Operation op : witness) {
                    log(String.format("    id=%-4d proc=%-2d %-3s arg=%-8s ret=%-8s outcome=%-13s invoke=%5dms complete=%5dms",
                            op.id(), op.processId(), op.type(), op.argValue(), op.returnedValue(), op.outcome(),
                            op.invokeNanos() / 1_000_000, op.completeNanos() / 1_000_000));
                }
            }
        }
        if (!allWitnesses.isEmpty()) {
            String violationsPath = historyPath.replaceFirst("\\.json$", "") + "-violations.json";
            writeHistoryJson(allWitnesses, violationsPath);
            log("Violation witness operations written to " + violationsPath);
        }
        log(allOk
                ? "RESULT: All keys linearizable. No consistency violations found across " + history.size() + " operations."
                : "RESULT: Linearizability VIOLATION detected — see minimal counterexample(s) above and full history in " + historyPath);

        System.exit(allOk ? 0 : 1);
    }

    private static void workerLoop(int processId, RawClient client, String[] keys, List<Operation> history,
                                    AtomicLong idGen, AtomicBoolean running, long testStartNanos) {
        while (running.get()) {
            String key = keys[ThreadLocalRandom.current().nextInt(keys.length)];
            boolean isWrite = ThreadLocalRandom.current().nextInt(100) < 30;

            long invoke = System.nanoTime() - testStartNanos;
            if (isWrite) {
                String value = String.valueOf(ThreadLocalRandom.current().nextInt(1_000_000));
                RawClient.Result r = client.put(key, value);
                long complete = System.nanoTime() - testStartNanos;
                history.add(new Operation(idGen.getAndIncrement(), processId, key, Operation.OperationType.PUT,
                        value, null, invoke, complete, r.outcome()));
            } else {
                RawClient.Result r = client.get(key);
                long complete = System.nanoTime() - testStartNanos;
                history.add(new Operation(idGen.getAndIncrement(), processId, key, Operation.OperationType.GET,
                        null, r.returnedValue(), invoke, complete, r.outcome()));
            }

            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(20));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void chaosLoop(ClusterConfig cluster, Map<Integer, List<ProcessHandle>> processHandles, String confPath,
                                   AtomicBoolean running, AtomicInteger chaosEvents,
                                   List<ChaosEvent> chaosEventLog, long testStartNanos) {
        while (running.get()) {
            sleepQuietly(ThreadLocalRandom.current().nextInt(3000, 6000));
            if (!running.get()) {
                return;
            }
            Integer leaderId = findLeader(cluster);
            if (leaderId == null) {
                continue;
            }
            List<ProcessHandle> handles = processHandles.get(leaderId);
            if (handles == null || handles.isEmpty()) {
                continue;
            }
            log("CHAOS: killing current leader node-" + leaderId + " (pid(s): "
                    + handles.stream().map(h -> String.valueOf(h.pid())).collect(Collectors.joining(", ")) + ")");
            for (ProcessHandle h : handles) {
                h.destroyForcibly();
            }
            chaosEvents.incrementAndGet();
            chaosEventLog.add(new ChaosEvent("kill", leaderId, (System.nanoTime() - testStartNanos) / 1_000_000));

            sleepQuietly(ThreadLocalRandom.current().nextInt(1000, 3000));
            if (!running.get()) {
                return;
            }

            boolean stillAlive = handles.stream().anyMatch(ProcessHandle::isAlive);
            if (stillAlive) {
                log("CHAOS: WARNING -- node-" + leaderId + " still has a live tracked process after destroyForcibly()");
            }
            try {
                List<ProcessHandle> restarted = startNodeTracked(leaderId, confPath);
                processHandles.put(leaderId, restarted);
                log("CHAOS: restarted node-" + leaderId);
                chaosEventLog.add(new ChaosEvent("restart", leaderId, (System.nanoTime() - testStartNanos) / 1_000_000));
            } catch (IOException e) {
                log("CHAOS: failed to restart node-" + leaderId + ": " + e.getMessage());
            }
        }
    }

    private static Integer findLeader(ClusterConfig cluster) {
        for (NodeConfig n : cluster.allNodes()) {
            if (RawClient.isLeader(n)) {
                return n.id();
            }
        }
        return null;
    }

    private static boolean waitForLeader(ClusterConfig cluster, int timeoutSec) throws InterruptedException {
        for (int i = 0; i < timeoutSec; i++) {
            if (findLeader(cluster) != null) {
                return true;
            }
            Thread.sleep(1000);
        }
        return false;
    }

    private static Process startNode(int id, String confPath) throws IOException {
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", "out", "com.raftkv.Server", String.valueOf(id), confPath);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(new File("chaos-logs/node" + id + ".log")));
        pb.redirectErrorStream(true);
        return pb.start();
    }

    private static List<ProcessHandle> startNodeTracked(int id, String confPath) throws IOException {
        Process started = startNode(id, confPath);
        List<ProcessHandle> handles = findProcessHandlesForNode(id);
        for (int attempt = 0; attempt < 25 && handles.isEmpty(); attempt++) {
            sleepQuietly(200);
            handles = findProcessHandlesForNode(id);
        }
        if (handles.isEmpty()) {
            handles = List.of(started.toHandle());
        } else if (handles.size() > 1) {
            log("   (found " + handles.size() + " OS processes for node " + id + " -- tracking all of them, "
                    + "likely a launcher/redirector such as Oracle's javapath)");
        }
        return handles;
    }

    private static List<ProcessHandle> findProcessHandlesForNode(int id) {
        String needle = "com.raftkv.Server " + id + " ";
        return ProcessHandle.allProcesses()
                .filter(ProcessHandle::isAlive)
                .filter(ph -> ph.info().commandLine().map(cmd -> cmd.contains(needle)).orElse(false))
                .collect(Collectors.toList());
    }

    private static void writeEventsJson(List<ChaosEvent> events, String path) throws IOException {
        try (PrintWriter w = new PrintWriter(new FileWriter(path))) {
            w.println("[");
            for (int i = 0; i < events.size(); i++) {
                ChaosEvent e = events.get(i);
                w.print("  {\"type\":\"" + e.type() + "\",\"nodeId\":" + e.nodeId() + ",\"atMs\":" + e.atMs() + "}");
                w.println(i < events.size() - 1 ? "," : "");
            }
            w.println("]");
        }
    }

    private static void writeHistoryJson(List<Operation> history, String path) throws IOException {
        try (PrintWriter w = new PrintWriter(new FileWriter(path))) {
            w.println("[");
            for (int i = 0; i < history.size(); i++) {
                Operation o = history.get(i);
                w.print("  {\"id\":" + o.id()
                        + ",\"process\":" + o.processId()
                        + ",\"key\":" + jsonStr(o.key())
                        + ",\"type\":\"" + o.type() + "\""
                        + ",\"arg\":" + jsonStr(o.argValue())
                        + ",\"returned\":" + jsonStr(o.returnedValue())
                        + ",\"invokeNanos\":" + o.invokeNanos()
                        + ",\"completeNanos\":" + o.completeNanos()
                        + ",\"outcome\":\"" + o.outcome() + "\"}");
                w.println(i < history.size() - 1 ? "," : "");
            }
            w.println("]");
        }
    }

    private static String jsonStr(String s) {
        if (s == null) {
            return "null";
        }
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void log(String message) {
        System.out.println("[" + LocalTime.now().format(FMT) + "][chaos-runner] " + message);
    }
}
