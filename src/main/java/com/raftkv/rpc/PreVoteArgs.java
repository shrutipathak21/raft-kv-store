package com.raftkv.rpc;

import java.io.Serializable;

public record PreVoteArgs(long term, int candidateId, int lastLogIndex, long lastLogTerm) implements Serializable {
}
