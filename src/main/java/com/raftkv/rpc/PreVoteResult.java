package com.raftkv.rpc;

import java.io.Serializable;

public record PreVoteResult(long term, boolean voteGranted) implements Serializable {
}
