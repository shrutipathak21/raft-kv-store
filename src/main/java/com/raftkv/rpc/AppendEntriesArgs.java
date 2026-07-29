package com.raftkv.rpc;

import com.raftkv.raft.LogEntry;

import java.io.Serializable;
import java.util.List;

public record AppendEntriesArgs(long term, int leaderId, int prevLogIndex, long prevLogTerm,
                                 List<LogEntry> entries, int leaderCommit) implements Serializable {

    public boolean isHeartbeat() {
        return entries.isEmpty();
    }
}
