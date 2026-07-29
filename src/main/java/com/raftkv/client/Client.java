package com.raftkv.client;

import com.raftkv.util.ClusterConfig;
import com.raftkv.util.NodeConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public final class Client {

    private static final int SOCKET_TIMEOUT_MS = 1000;
    private static final int MAX_ATTEMPTS = 10;
    private static final long RETRY_DELAY_MS = 300;

    private final ClusterConfig cluster;

    public Client(ClusterConfig cluster) {
        this.cluster = cluster;
    }

    public record Result(int nodeId, String response) {
    }

    public Result send(String request, int preferredNodeId) {
        int nodeId = preferredNodeId;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            NodeConfig target = nodeId > 0 ? cluster.get(nodeId) : cluster.allNodes().get(attempt % cluster.size());
            try {
                String response = sendTo(target, request);
                if (response.startsWith("NOT_LEADER")) {
                    String[] addr = response.split("\\s+")[1].split(":");
                    NodeConfig redirected = findByAddress(addr[0], Integer.parseInt(addr[1]));
                    if (redirected != null) {
                        nodeId = redirected.id();
                        continue;
                    }
                }
                if (response.equals("NO_LEADER")) {
                    sleep();
                    nodeId = -1;
                    continue;
                }
                return new Result(target.id(), response);
            } catch (IOException e) {
                nodeId = -1;
                sleep();
            }
        }
        return new Result(-1, "ERROR client gave up after " + MAX_ATTEMPTS + " attempts");
    }

    private NodeConfig findByAddress(String host, int port) {
        return cluster.allNodes().stream()
                .filter(n -> n.host().equals(host) && n.clientPort() == port)
                .findFirst().orElse(null);
    }

    private String sendTo(NodeConfig target, String request) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(target.host(), target.clientPort()), SOCKET_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
            writer.println(request);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String response = reader.readLine();
            return response == null ? "ERROR empty response" : response;
        }
    }

    private void sleep() {
        try {
            Thread.sleep(RETRY_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: Client <clusterConfigPath> <preferredNodeId|0> <PUT key value | GET key | STATUS | WHOISLEADER>");
            System.exit(1);
        }
        ClusterConfig cluster = ClusterConfig.load(args[0]);
        int preferredNodeId = Integer.parseInt(args[1]);
        String command = args[2];
        Client client = new Client(cluster);

        if (command.equalsIgnoreCase("WHOISLEADER")) {
            Result result = client.send("GET __whoisleader_probe__", preferredNodeId > 0 ? preferredNodeId : -1);
            if (result.response().startsWith("ERROR") || result.nodeId() < 0) {
                System.out.println("NO_LEADER");
            } else {
                System.out.println("LEADER " + result.nodeId());
            }
            return;
        }

        String request = String.join(" ", java.util.Arrays.copyOfRange(args, 2, args.length));
        Result result = client.send(request, preferredNodeId > 0 ? preferredNodeId : -1);
        System.out.println(result.response());
    }
}
