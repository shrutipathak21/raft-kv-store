package com.raftkv;

import com.raftkv.client.ClientApiServer;
import com.raftkv.kv.KVStateMachine;
import com.raftkv.raft.RaftNode;
import com.raftkv.rpc.RpcServer;
import com.raftkv.util.ClusterConfig;
import com.raftkv.util.Logger;
import com.raftkv.util.NodeConfig;

public final class Server {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Server <nodeId> <clusterConfigPath>");
            System.exit(1);
        }
        int nodeId = Integer.parseInt(args[0]);
        ClusterConfig cluster = ClusterConfig.load(args[1]);
        NodeConfig self = cluster.get(nodeId);

        KVStateMachine stateMachine = new KVStateMachine();
        RaftNode raftNode = new RaftNode(nodeId, cluster, stateMachine);

        RpcServer rpcServer = new RpcServer(raftNode, nodeId, self.raftPort());
        ClientApiServer clientApiServer = new ClientApiServer(raftNode, cluster, self.clientPort());

        rpcServer.start();
        clientApiServer.start();
        raftNode.start();

        Logger.log(nodeId, "Node fully initialized. raftPort=" + self.raftPort() + " clientPort=" + self.clientPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Logger.log(nodeId, "Shutting down.");
            rpcServer.shutdown();
            clientApiServer.shutdown();
            raftNode.shutdown();
        }));

        Thread.currentThread().join();
    }
}
