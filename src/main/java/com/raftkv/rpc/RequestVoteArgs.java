package com.raftkv.rpc;

import java.io.Serializable;

public record RequestVoteArgs(long term, int candidateId, int lastLogIndex, long lastLogTerm)
        implements Serializable {
}
