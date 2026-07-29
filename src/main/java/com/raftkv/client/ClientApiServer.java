package com.raftkv.client;

import com.raftkv.kv.Command;
import com.raftkv.raft.RaftNode;
import com.raftkv.util.ClusterConfig;
import com.raftkv.util.Logger;
import com.raftkv.util.NodeConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientApiServer implements Runnable {

    private static final int COMMIT_TIMEOUT_MS = 2000;

    private final RaftNode raftNode;
    private final ClusterConfig cluster;
    private final int port;

    private final ExecutorService pool = Executors.newFixedThreadPool(32);
    private volatile boolean running = false;
    private ServerSocketChannel serverChannel;

    public ClientApiServer(RaftNode raftNode, ClusterConfig cluster, int port) {
        this.raftNode = raftNode;
        this.cluster = cluster;
        this.port = port;
    }

    public void start() {
        running = true;
        Thread t = new Thread(this, "client-api-server-" + port);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void run() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(port));
            Logger.log(raftNode.getId(), "Client API listening on port " + port);
            while (running) {
                SocketChannel client = serverChannel.accept();
                pool.submit(() -> handle(client));
            }
        } catch (IOException e) {
            if (running) {
                Logger.log(raftNode.getId(), "Client API server error: " + e.getMessage());
            }
        }
    }

    private void handle(SocketChannel channel) {
        try (Socket socket = channel.socket();
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8)) {

            String line = reader.readLine();
            if (line == null) {
                return;
            }
            writer.println(dispatch(line.trim()));
        } catch (IOException e) {

        }
    }

    private String dispatch(String line) {
        String[] parts = line.split("\\s+", 3);
        if (parts.length == 0) {
            return "ERROR empty request";
        }
        String cmd = parts[0].toUpperCase();
        try {
            return switch (cmd) {
                case "PUT" -> handlePut(parts);
                case "GET" -> handleGet(parts);
                case "STATUS" -> raftNode.statusString();
                default -> "ERROR unknown command " + cmd;
            };
        } catch (Exception e) {
            return "ERROR " + e.getMessage();
        }
    }

    private String handlePut(String[] parts) {
        if (parts.length < 3) {
            return "ERROR usage: PUT key value";
        }
        String key = parts[1];
        String value = parts[2];

        if (raftNode.getLeaderId() == -1) {
            return "NO_LEADER";
        }
        if (!raftNode.isLeader()) {
            return redirect();
        }

        Command command = Command.put(key, value, UUID.randomUUID().toString());
        try {
            String result = raftNode.submitCommand(command).get(COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return "OK " + result;
        } catch (TimeoutException e) {
            return "TIMEOUT not committed within " + COMMIT_TIMEOUT_MS + "ms (possible leadership change)";
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                return redirect();
            }
            return "ERROR " + e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "ERROR interrupted";
        }
    }

    private String handleGet(String[] parts) {
        if (parts.length < 2) {
            return "ERROR usage: GET key";
        }
        String key = parts[1];

        if (raftNode.getLeaderId() == -1) {
            return "NO_LEADER";
        }
        if (!raftNode.isLeader()) {
            return redirect();
        }
        if (!raftNode.isReadyToServeReads()) {

            return "NO_LEADER";
        }
        try {

            raftNode.confirmLeadership().get(COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return "NO_LEADER";
        }
        String value = raftNode.getLocalValue(key);
        return value == null ? "NOT_FOUND" : "VALUE " + value;
    }

    private String redirect() {
        int leaderId = raftNode.getLeaderId();
        if (leaderId == -1) {
            return "NO_LEADER";
        }
        NodeConfig leader = cluster.get(leaderId);
        return "NOT_LEADER " + leader.clientAddress();
    }

    public void shutdown() {
        running = false;
        try {
            if (serverChannel != null) {
                serverChannel.close();
            }
        } catch (IOException ignored) {
        }
        pool.shutdownNow();
    }
}
