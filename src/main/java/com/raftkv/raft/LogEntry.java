package com.raftkv.raft;

import com.raftkv.kv.Command;

import java.io.Serializable;

public record LogEntry(long term, int index, Command command) implements Serializable {

    public static LogEntry sentinel() {
        return new LogEntry(0, 0, null);
    }
}
