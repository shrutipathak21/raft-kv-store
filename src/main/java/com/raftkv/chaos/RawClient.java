package com.raftkv.chaos;

import com.raftkv.util.ClusterConfig;
import com.raftkv.util.NodeConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class RawClient {

    private static final int SOCKET_TIMEOUT_MS = 2500;
    private static final int MAX_REDIRECT_HOPS = 4;

    private final ClusterConfig cluster;
    private final AtomicInteger lastKnownLeader = new AtomicInteger(-1);

    public RawClient(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    public record Result(Operation.Outcome outcome, String returnedValue) {
    }

    public Result put(String key, String value) {
        return execute("PUT " + key + " " + value, true);
    }

    public Result get(String key) {
        return execute("GET " + key, false);
    }

    private Result execute(String request, boolean isWrite) {
        int currentId = lastKnownLeader.get();
        if (currentId <= 0) {
            currentId = 1 + ThreadLocalRandom.current().nextInt(cluster.size());
        }

        for (int hop = 0; hop < MAX_REDIRECT_HOPS; hop++) {
            NodeConfig target = cluster.get(currentId);
            try {
                String response = sendRaw(target, request);

                if (response.startsWith("NOT_LEADER")) {
                    String addr = response.split("\\s+")[1];
                    int redirectedId = resolveByClientAddress(addr);
                    if (redirectedId <= 0) {
                        return new Result(Operation.Outcome.FAIL, null);
                    }
                    currentId = redirectedId;
                    continue;
                }
                if (response.equals("NO_LEADER")) {

                    return new Result(Operation.Outcome.FAIL, null);
                }
                if (response.startsWith("TIMEOUT")) {

                    return new Result(Operation.Outcome.INDETERMINATE, null);
                }

                lastKnownLeader.set(currentId);
                if (response.startsWith("OK")) {
                    return new Result(Operation.Outcome.OK, null);
                }
                if (response.startsWith("VALUE ")) {
                    return new Result(Operation.Outcome.OK, response.substring("VALUE ".length()));
                }
                if (response.equals("NOT_FOUND")) {
                    return new Result(Operation.Outcome.OK, null);
                }
                return new Result(Operation.Outcome.FAIL, null);
            } catch (ConnectException e) {

                lastKnownLeader.set(-1);
                return new Result(Operation.Outcome.FAIL, null);
            } catch (IOException e) {

                lastKnownLeader.set(-1);
                return new Result(isWrite ? Operation.Outcome.INDETERMINATE : Operation.Outcome.FAIL, null);
            }
        }

        return new Result(isWrite ? Operation.Outcome.INDETERMINATE : Operation.Outcome.FAIL, null);
    }

    private int resolveByClientAddress(String hostColonPort) {
        String[] parts = hostColonPort.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return cluster.allNodes().stream()
                .filter(n -> n.host().equals(host) && n.clientPort() == port)
                .map(NodeConfig::id)
                .findFirst().orElse(-1);
    }

    private String sendRaw(NodeConfig target, String request) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(target.host(), target.clientPort()), SOCKET_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
            writer.println(request);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String response = reader.readLine();
            if (response == null) {
                throw new IOException("connection closed with no response");
            }
            return response;
        }
    }

    public static boolean isLeader(NodeConfig node) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(node.host(), node.clientPort()), 800);
            socket.setSoTimeout(800);
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
            writer.println("STATUS");
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String response = reader.readLine();
            return response != null && response.contains("state=LEADER");
        } catch (IOException e) {
            return false;
        }
    }
}
