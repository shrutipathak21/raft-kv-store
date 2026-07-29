package com.raftkv.rpc;

import java.io.Serializable;

public record RequestVoteResult(long term, boolean voteGranted) implements Serializable {
}
