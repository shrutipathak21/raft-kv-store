package com.raftkv.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class ClusterConfig {

    private final List<NodeConfig> nodes;

    private ClusterConfig(List<NodeConfig> nodes) {
        this.nodes = nodes;
    }

    public static ClusterConfig load(String path) throws IOException {
        List<NodeConfig> nodes = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(Path.of(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split(",");
                int id = Integer.parseInt(parts[0].trim());
                String host = parts[1].trim();
                int raftPort = Integer.parseInt(parts[2].trim());
                int clientPort = Integer.parseInt(parts[3].trim());
                nodes.add(new NodeConfig(id, host, raftPort, clientPort));
            }
        }
        return new ClusterConfig(nodes);
    }

    public List<NodeConfig> allNodes() {
        return nodes;
    }

    public List<NodeConfig> peersOf(int selfId) {
        return nodes.stream().filter(n -> n.id() != selfId).toList();
    }

    public NodeConfig get(int id) {
        return nodes.stream().filter(n -> n.id() == id).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown node id " + id));
    }

    public int size() {
        return nodes.size();
    }

    public int majority() {
        return (nodes.size() / 2) + 1;
    }
}
