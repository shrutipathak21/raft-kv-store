package com.raftkv.rpc;

import java.io.Serializable;

public record AppendEntriesResult(long term, boolean success, int matchIndex, int conflictIndex, long conflictTerm)
        implements Serializable {
}
