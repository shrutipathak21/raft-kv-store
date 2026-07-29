package com.raftkv.util;

import java.io.Serializable;

public record NodeConfig(int id, String host, int raftPort, int clientPort) implements Serializable {
    public String raftAddress() {
        return host + ":" + raftPort;
    }

    public String clientAddress() {
        return host + ":" + clientPort;
    }
}
