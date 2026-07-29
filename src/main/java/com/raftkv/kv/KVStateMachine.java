package com.raftkv.kv;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVStateMachine {

    private final Map<String, String> store = new ConcurrentHashMap<>();

    public String apply(Command command) {
        if (command == null || command.type() == Command.Type.NOOP) {
            return null;
        }
        store.put(command.key(), command.value());
        return "OK";
    }

    public String get(String key) {
        return store.get(key);
    }

    public int size() {
        return store.size();
    }
}
