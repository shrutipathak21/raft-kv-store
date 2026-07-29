package com.raftkv.kv;

import java.io.Serializable;

public record Command(Type type, String key, String value, String requestId) implements Serializable {

    public enum Type { PUT, NOOP }

    public static Command put(String key, String value, String requestId) {
        return new Command(Type.PUT, key, value, requestId);
    }

    public static Command noop() {
        return new Command(Type.NOOP, null, null, null);
    }
}
