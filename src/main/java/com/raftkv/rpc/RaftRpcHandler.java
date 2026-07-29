package com.raftkv.rpc;

public interface RaftRpcHandler {
    RequestVoteResult handleRequestVote(RequestVoteArgs args);

    AppendEntriesResult handleAppendEntries(AppendEntriesArgs args);

    PreVoteResult handlePreVote(PreVoteArgs args);
}
