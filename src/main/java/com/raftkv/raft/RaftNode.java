package com.raftkv.raft;

import com.raftkv.kv.Command;
import com.raftkv.kv.KVStateMachine;
import com.raftkv.rpc.AppendEntriesArgs;
import com.raftkv.rpc.AppendEntriesResult;
import com.raftkv.rpc.PreVoteArgs;
import com.raftkv.rpc.PreVoteResult;
import com.raftkv.rpc.RaftRpcHandler;
import com.raftkv.rpc.RequestVoteArgs;
import com.raftkv.rpc.RequestVoteResult;
import com.raftkv.rpc.RpcClient;
import com.raftkv.util.ClusterConfig;
import com.raftkv.util.Logger;
import com.raftkv.util.NodeConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode implements RaftRpcHandler {

    private static final int HEARTBEAT_INTERVAL_MS = 400;
    private static final int ELECTION_TIMEOUT_MIN_MS = 2500;
    private static final int ELECTION_TIMEOUT_MAX_MS = 5000;
    private static final int RPC_TIMEOUT_MS = 1500;

    private final int id;
    private final ClusterConfig cluster;
    private final List<NodeConfig> peers;
    private final KVStateMachine stateMachine;

    private final ReentrantLock lock = new ReentrantLock();
    private final Random random = new Random();

    private volatile RaftState state = RaftState.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile Integer votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();

    private volatile int commitIndex = 0;
    private volatile int lastApplied = 0;
    private volatile int leaderId = -1;

    private final Map<Integer, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> matchIndex = new ConcurrentHashMap<>();

    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(4);

    private final ExecutorService rpcExecutor = Executors.newFixedThreadPool(16);
    private volatile ScheduledFuture<?> electionTimerFuture;
    private volatile ScheduledFuture<?> heartbeatFuture;

    private final Map<Integer, java.util.concurrent.atomic.AtomicBoolean> heartbeatInFlight = new ConcurrentHashMap<>();

    private volatile int currentTermBarrierIndex = -1;

    private volatile long lastLeaderContactNanos = System.nanoTime();

    private final Map<Integer, CompletableFuture<String>> pendingClientRequests = new ConcurrentHashMap<>();

    public RaftNode(int id, ClusterConfig cluster, KVStateMachine stateMachine) {
        this.id = id;
        this.cluster = cluster;
        this.peers = cluster.peersOf(id);
        this.stateMachine = stateMachine;
        this.log.add(LogEntry.sentinel());
        for (NodeConfig peer : peers) {
            heartbeatInFlight.put(peer.id(), new java.util.concurrent.atomic.AtomicBoolean(false));
        }
    }

    public void start() {
        Logger.event(id, "BOOT", "Node started as FOLLOWER, term=0, cluster size=" + cluster.size());
        resetElectionTimer();
    }

    public void shutdown() {
        timers.shutdownNow();
        rpcExecutor.shutdownNow();
    }

    private int randomElectionTimeoutMs() {
        return ELECTION_TIMEOUT_MIN_MS + random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
    }

    private void resetElectionTimer() {
        if (electionTimerFuture != null) {
            electionTimerFuture.cancel(false);
        }
        int timeoutMs = randomElectionTimeoutMs();
        electionTimerFuture = timers.schedule(this::onElectionTimeout, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimer() {
        if (electionTimerFuture != null) {
            electionTimerFuture.cancel(false);
        }
    }

    private void onElectionTimeout() {
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                return;
            }
            resetElectionTimer();
        } finally {
            lock.unlock();
        }
        startPreVote();
    }

    private void startPreVote() {
        long proposedTerm;
        int lastLogIndex;
        long lastLogTerm;

        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                return;
            }
            proposedTerm = currentTerm + 1;
            lastLogIndex = log.size() - 1;
            lastLogTerm = log.get(lastLogIndex).term();
        } finally {
            lock.unlock();
        }

        Logger.event(id, "ELECTION", "Pre-vote: probing for term " + proposedTerm + " before committing to a real election");

        AtomicInteger grantsReceived = new AtomicInteger(1);
        java.util.concurrent.atomic.AtomicBoolean decided = new java.util.concurrent.atomic.AtomicBoolean(false);
        PreVoteArgs args = new PreVoteArgs(proposedTerm, id, lastLogIndex, lastLogTerm);

        if (grantsReceived.get() >= cluster.majority() && decided.compareAndSet(false, true)) {
            startElection();
            return;
        }

        for (NodeConfig peer : peers) {
            rpcExecutor.submit(() -> {
                try {
                    Object response = RpcClient.send(peer, args, RPC_TIMEOUT_MS);
                    if (response instanceof PreVoteResult result && result.voteGranted()) {
                        int grants = grantsReceived.incrementAndGet();
                        if (grants >= cluster.majority() && decided.compareAndSet(false, true)) {
                            Logger.event(id, "ELECTION", "Pre-vote succeeded for term " + proposedTerm
                                    + " (" + grants + "/" + cluster.size() + ") — proceeding to a real election");
                            startElection();
                        }
                    }
                } catch (Exception e) {

                }
            });
        }
    }

    private void startElection() {
        long termForThisElection;
        int lastLogIndex;
        long lastLogTerm;

        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                return;
            }
            currentTerm++;
            state = RaftState.CANDIDATE;
            votedFor = id;
            leaderId = -1;
            termForThisElection = currentTerm;
            lastLogIndex = log.size() - 1;
            lastLogTerm = log.get(lastLogIndex).term();
            resetElectionTimer();
            Logger.event(id, "ELECTION", "Starting election for term " + termForThisElection
                    + " (lastLogIndex=" + lastLogIndex + ", lastLogTerm=" + lastLogTerm + ")");
        } finally {
            lock.unlock();
        }

        AtomicInteger votesReceived = new AtomicInteger(1);
        RequestVoteArgs args = new RequestVoteArgs(termForThisElection, id, lastLogIndex, lastLogTerm);

        for (NodeConfig peer : peers) {
            rpcExecutor.submit(() -> {
                try {
                    Object response = RpcClient.send(peer, args, RPC_TIMEOUT_MS);
                    if (response instanceof RequestVoteResult result) {
                        handleRequestVoteResponse(termForThisElection, result, votesReceived);
                    }
                } catch (Exception e) {

                }
            });
        }
    }

    private void handleRequestVoteResponse(long electionTerm, RequestVoteResult result, AtomicInteger votesReceived) {
        lock.lock();
        try {
            if (result.term() > currentTerm) {
                stepDown(result.term());
                return;
            }
            if (state != RaftState.CANDIDATE || currentTerm != electionTerm) {
                return;
            }
            if (result.voteGranted()) {
                int votes = votesReceived.incrementAndGet();
                Logger.event(id, "ELECTION", "Received vote grant for term " + electionTerm
                        + " (" + votes + "/" + cluster.size() + ")");
                if (votes >= cluster.majority()) {
                    becomeLeader();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RequestVoteResult handleRequestVote(RequestVoteArgs args) {
        lock.lock();
        try {
            if (args.term() < currentTerm) {
                return new RequestVoteResult(currentTerm, false);
            }
            if (args.term() > currentTerm) {
                stepDown(args.term());
            }

            int myLastLogIndex = log.size() - 1;
            long myLastLogTerm = log.get(myLastLogIndex).term();
            boolean candidateLogUpToDate =
                    args.lastLogTerm() > myLastLogTerm
                            || (args.lastLogTerm() == myLastLogTerm && args.lastLogIndex() >= myLastLogIndex);

            boolean canVote = (votedFor == null || votedFor == args.candidateId());

            if (canVote && candidateLogUpToDate) {
                votedFor = args.candidateId();
                resetElectionTimer();
                Logger.event(id, "ELECTION", "Granted vote to node-" + args.candidateId() + " for term " + args.term());
                return new RequestVoteResult(currentTerm, true);
            }
            return new RequestVoteResult(currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PreVoteResult handlePreVote(PreVoteArgs args) {
        lock.lock();
        try {
            boolean termOk = args.term() >= currentTerm;

            int myLastLogIndex = log.size() - 1;
            long myLastLogTerm = log.get(myLastLogIndex).term();
            boolean candidateLogUpToDate =
                    args.lastLogTerm() > myLastLogTerm
                            || (args.lastLogTerm() == myLastLogTerm && args.lastLogIndex() >= myLastLogIndex);

            boolean heardFromLeaderRecently =
                    (System.nanoTime() - lastLeaderContactNanos) < TimeUnit.MILLISECONDS.toNanos(ELECTION_TIMEOUT_MIN_MS);

            boolean grant = termOk && candidateLogUpToDate && !heardFromLeaderRecently;
            return new PreVoteResult(currentTerm, grant);
        } finally {
            lock.unlock();
        }
    }

    private void becomeLeader() {
        if (state != RaftState.CANDIDATE) {
            return;
        }
        state = RaftState.LEADER;
        leaderId = id;
        cancelElectionTimer();

        int lastLogIndex = log.size() - 1;
        for (NodeConfig peer : peers) {
            nextIndex.put(peer.id(), lastLogIndex + 1);
            matchIndex.put(peer.id(), 0);
        }

        Logger.event(id, "LEADER", "*** Became LEADER for term " + currentTerm + " ***");

        currentTermBarrierIndex = log.size();
        appendToLocalLog(Command.noop());

        heartbeatFuture = timers.scheduleAtFixedRate(this::sendHeartbeats, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void cancelHeartbeatTimer() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
        }
    }

    private void stepDown(long newTerm) {
        if (newTerm > currentTerm) {
            currentTerm = newTerm;
            votedFor = null;
        }
        if (state != RaftState.FOLLOWER) {
            Logger.event(id, "TERM", "Stepping down to FOLLOWER (term=" + newTerm + ")");
        }
        state = RaftState.FOLLOWER;
        leaderId = -1;
        cancelHeartbeatTimer();
        resetElectionTimer();
    }

    private void sendHeartbeats() {

        if (state != RaftState.LEADER) {
            cancelHeartbeatTimer();
            return;
        }
        for (NodeConfig peer : peers) {
            java.util.concurrent.atomic.AtomicBoolean inFlight = heartbeatInFlight.get(peer.id());
            if (inFlight.compareAndSet(false, true)) {
                rpcExecutor.submit(() -> {
                    try {
                        replicateTo(peer);
                    } finally {
                        inFlight.set(false);
                    }
                });
            }

        }
    }

    private void replicateTo(NodeConfig peer) {
        long term;
        int prevLogIndex;
        long prevLogTerm;
        List<LogEntry> entries;
        int leaderCommitSnapshot;

        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                return;
            }
            term = currentTerm;
            int ni = nextIndex.getOrDefault(peer.id(), log.size());
            prevLogIndex = ni - 1;
            prevLogTerm = log.get(prevLogIndex).term();
            entries = ni < log.size() ? new ArrayList<>(log.subList(ni, log.size())) : List.of();
            leaderCommitSnapshot = commitIndex;
        } finally {
            lock.unlock();
        }

        AppendEntriesArgs args = new AppendEntriesArgs(term, id, prevLogIndex, prevLogTerm, entries, leaderCommitSnapshot);

        try {
            Object response = RpcClient.send(peer, args, RPC_TIMEOUT_MS);
            if (response instanceof AppendEntriesResult result) {
                handleAppendEntriesResponse(peer, term, prevLogIndex, entries.size(), result);
            }
        } catch (Exception e) {

        }
    }

    private void handleAppendEntriesResponse(NodeConfig peer, long sentTerm, int prevLogIndex, int entryCount,
                                              AppendEntriesResult result) {
        lock.lock();
        try {
            if (result.term() > currentTerm) {
                stepDown(result.term());
                return;
            }
            if (state != RaftState.LEADER || currentTerm != sentTerm) {
                return;
            }
            if (result.success()) {
                int newMatchIndex = prevLogIndex + entryCount;
                matchIndex.merge(peer.id(), newMatchIndex, Math::max);
                nextIndex.put(peer.id(), newMatchIndex + 1);
                updateCommitIndex();
            } else {

                int newNextIndex = result.conflictTerm() < 0
                        ? Math.max(1, result.conflictIndex())
                        : findLastIndexOfTerm(result.conflictTerm())
                                .map(idx -> idx + 1)
                                .orElse(Math.max(1, result.conflictIndex()));
                nextIndex.put(peer.id(), Math.min(newNextIndex, log.size()));

                rpcExecutor.submit(() -> replicateTo(peer));
            }
        } finally {
            lock.unlock();
        }
    }

    private java.util.Optional<Integer> findLastIndexOfTerm(long term) {
        for (int i = log.size() - 1; i >= 1; i--) {
            if (log.get(i).term() == term) {
                return java.util.Optional.of(i);
            }
        }
        return java.util.Optional.empty();
    }

    private void updateCommitIndex() {
        int lastLogIndex = log.size() - 1;
        for (int nIdx = lastLogIndex; nIdx > commitIndex; nIdx--) {
            if (log.get(nIdx).term() != currentTerm) {
                continue;
            }
            int replicatedCount = 1;
            for (NodeConfig peer : peers) {
                if (matchIndex.getOrDefault(peer.id(), 0) >= nIdx) {
                    replicatedCount++;
                }
            }
            if (replicatedCount >= cluster.majority()) {
                commitIndex = nIdx;
                Logger.event(id, "COMMIT", "commitIndex advanced to " + nIdx + " (term " + currentTerm + ")");
                applyCommittedEntries();
                break;
            }
        }
    }

    @Override
    public AppendEntriesResult handleAppendEntries(AppendEntriesArgs args) {
        lock.lock();
        try {
            if (args.term() < currentTerm) {
                return new AppendEntriesResult(currentTerm, false, 0, log.size(), -1);
            }

            stepDown(args.term());
            leaderId = args.leaderId();
            resetElectionTimer();
            lastLeaderContactNanos = System.nanoTime();

            if (args.prevLogIndex() >= log.size()) {

                return new AppendEntriesResult(currentTerm, false, 0, log.size(), -1);
            }

            long ourTermAtPrev = log.get(args.prevLogIndex()).term();
            if (ourTermAtPrev != args.prevLogTerm()) {

                long conflictTerm = ourTermAtPrev;
                int conflictIndex = args.prevLogIndex();
                while (conflictIndex > 1 && log.get(conflictIndex - 1).term() == conflictTerm) {
                    conflictIndex--;
                }
                truncateLogFrom(args.prevLogIndex());
                return new AppendEntriesResult(currentTerm, false, 0, conflictIndex, conflictTerm);
            }

            int index = args.prevLogIndex();
            for (LogEntry incoming : args.entries()) {
                index++;
                if (index < log.size()) {
                    if (log.get(index).term() != incoming.term()) {
                        truncateLogFrom(index);
                        log.add(incoming);
                    }
                } else {
                    log.add(incoming);
                }
            }

            if (args.leaderCommit() > commitIndex) {
                commitIndex = Math.min(args.leaderCommit(), log.size() - 1);
                applyCommittedEntries();
            }

            int matchIdx = args.prevLogIndex() + args.entries().size();
            return new AppendEntriesResult(currentTerm, true, matchIdx, 0, -1);
        } finally {
            lock.unlock();
        }
    }

    private void truncateLogFrom(int fromIndex) {
        if (fromIndex < log.size()) {
            log.subList(fromIndex, log.size()).clear();
        }
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            String result = stateMachine.apply(entry.command());
            CompletableFuture<String> pending = pendingClientRequests.remove(entry.index());
            if (pending != null) {
                pending.complete(result == null ? "OK" : result);
            }
            if (entry.command() != null && entry.command().type() == Command.Type.PUT) {
                Logger.event(id, "APPLY", "Applied entry " + entry.index() + " (term " + entry.term()
                        + "): PUT " + entry.command().key() + "=" + entry.command().value());
            }
        }
    }

    public CompletableFuture<String> submitCommand(Command command) {
        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                CompletableFuture<String> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("NOT_LEADER"));
                return failed;
            }
            return appendToLocalLog(command);
        } finally {
            lock.unlock();
        }
    }

    private CompletableFuture<String> appendToLocalLog(Command command) {
        int newIndex = log.size();
        LogEntry entry = new LogEntry(currentTerm, newIndex, command);
        log.add(entry);

        CompletableFuture<String> future = new CompletableFuture<>();
        if (command.type() != Command.Type.NOOP) {
            pendingClientRequests.put(newIndex, future);
            Logger.event(id, "REPLICATE", "Leader appended entry " + newIndex + " (term " + currentTerm + "): "
                    + command.type() + " " + command.key());
        } else {
            future.complete("OK");
        }

        for (NodeConfig peer : peers) {
            rpcExecutor.submit(() -> replicateTo(peer));
        }
        return future;
    }

    public boolean isLeader() {
        return state == RaftState.LEADER;
    }

    public boolean isReadyToServeReads() {
        return state == RaftState.LEADER && commitIndex >= currentTermBarrierIndex;
    }

    public CompletableFuture<Void> confirmLeadership() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        long myTerm;

        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                future.completeExceptionally(new IllegalStateException("NOT_LEADER"));
                return future;
            }
            myTerm = currentTerm;
        } finally {
            lock.unlock();
        }

        AtomicInteger acks = new AtomicInteger(1);
        java.util.concurrent.atomic.AtomicBoolean decided = new java.util.concurrent.atomic.AtomicBoolean(false);

        if (acks.get() >= cluster.majority() && decided.compareAndSet(false, true)) {
            future.complete(null);
            return future;
        }

        for (NodeConfig peer : peers) {
            rpcExecutor.submit(() -> confirmLeadershipWithPeer(peer, myTerm, acks, decided, future));
        }

        timers.schedule(() -> {
            if (decided.compareAndSet(false, true)) {
                future.completeExceptionally(new java.util.concurrent.TimeoutException(
                        "could not confirm leadership from a live majority in time"));
            }
        }, RPC_TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS);

        return future;
    }

    private void confirmLeadershipWithPeer(NodeConfig peer, long myTerm, AtomicInteger acks,
                                            java.util.concurrent.atomic.AtomicBoolean decided,
                                            CompletableFuture<Void> future) {
        int prevLogIndex;
        long prevLogTerm;
        int leaderCommitSnapshot;
        lock.lock();
        try {
            if (state != RaftState.LEADER || currentTerm != myTerm) {
                return;
            }
            prevLogIndex = log.size() - 1;
            prevLogTerm = log.get(prevLogIndex).term();
            leaderCommitSnapshot = commitIndex;
        } finally {
            lock.unlock();
        }

        AppendEntriesArgs args = new AppendEntriesArgs(myTerm, id, prevLogIndex, prevLogTerm, List.of(), leaderCommitSnapshot);
        try {
            Object response = RpcClient.send(peer, args, RPC_TIMEOUT_MS);
            if (!(response instanceof AppendEntriesResult result)) {
                return;
            }
            if (result.term() > myTerm) {

                lock.lock();
                try {
                    stepDown(result.term());
                } finally {
                    lock.unlock();
                }
                if (decided.compareAndSet(false, true)) {
                    future.completeExceptionally(new IllegalStateException("NOT_LEADER"));
                }
                return;
            }

            int count = acks.incrementAndGet();
            if (count >= cluster.majority() && decided.compareAndSet(false, true)) {
                future.complete(null);
            }
        } catch (Exception e) {

        }
    }

    public int getLeaderId() {
        return leaderId;
    }

    public String getLocalValue(String key) {
        return stateMachine.get(key);
    }

    public int getId() {
        return id;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public RaftState getState() {
        return state;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public int getLogSize() {
        return log.size();
    }

    public String statusString() {
        return "id=" + id + " state=" + state + " term=" + currentTerm + " leaderId=" + leaderId
                + " commitIndex=" + commitIndex + " lastApplied=" + lastApplied + " logSize=" + log.size();
    }
}
