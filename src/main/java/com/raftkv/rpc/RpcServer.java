package com.raftkv.rpc;

import com.raftkv.util.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcServer implements Runnable {

    private final RaftRpcHandler handler;
    private final int port;
    private final int nodeId;

    private final ExecutorService connectionPool = Executors.newFixedThreadPool(32);
    private volatile boolean running = false;
    private ServerSocketChannel serverChannel;

    public RpcServer(RaftRpcHandler handler, int nodeId, int port) {
        this.handler = handler;
        this.nodeId = nodeId;
        this.port = port;
    }

    public void start() {
        running = true;
        Thread t = new Thread(this, "raft-rpc-server-" + port);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void run() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(port));
            Logger.log(nodeId, "Raft RPC server listening on port " + port);
            while (running) {
                SocketChannel client = serverChannel.accept();
                connectionPool.submit(() -> handleConnection(client));
            }
        } catch (IOException e) {
            if (running) {
                Logger.log(nodeId, "RPC server error: " + e.getMessage());
            }
        }
    }

    private void handleConnection(SocketChannel clientChannel) {
        try (Socket socket = clientChannel.socket()) {
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Object request = in.readObject();
            Object response;

            if (request instanceof RequestVoteArgs args) {
                response = handler.handleRequestVote(args);
            } else if (request instanceof AppendEntriesArgs args) {
                response = handler.handleAppendEntries(args);
            } else if (request instanceof PreVoteArgs args) {
                response = handler.handlePreVote(args);
            } else {
                response = null;
            }

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(response);
            out.flush();
        } catch (IOException | ClassNotFoundException e) {

        }
    }

    public void shutdown() {
        running = false;
        try {
            if (serverChannel != null) {
                serverChannel.close();
            }
        } catch (IOException ignored) {
        }
        connectionPool.shutdownNow();
    }
}
