package com.raftkv.util;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public final class Logger {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Logger() {
    }

    public static void log(int nodeId, String message) {
        System.out.println("[" + LocalTime.now().format(FMT) + "][node-" + nodeId + "] " + message);
    }

    public static void event(int nodeId, String category, String message) {
        System.out.println("[" + LocalTime.now().format(FMT) + "][node-" + nodeId + "][" + category + "] " + message);
    }
}
