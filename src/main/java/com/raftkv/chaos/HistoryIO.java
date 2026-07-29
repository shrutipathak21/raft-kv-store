package com.raftkv.chaos;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HistoryIO {

    private static final Pattern FIELD = Pattern.compile(
            "\"id\":(\\d+),\"process\":(\\d+),\"key\":(\"[^\"]*\"|null),\"type\":\"(\\w+)\"," +
            "\"arg\":(\"(?:[^\"\\\\]|\\\\.)*\"|null),\"returned\":(\"(?:[^\"\\\\]|\\\\.)*\"|null)," +
            "\"invokeNanos\":(\\d+),\"completeNanos\":(\\d+),\"outcome\":\"(\\w+)\"");

    private HistoryIO() {
    }

    public static List<Operation> load(String path) throws IOException {
        String content = Files.readString(Path.of(path));
        List<Operation> ops = new ArrayList<>();
        Matcher m = FIELD.matcher(content);
        while (m.find()) {
            long id = Long.parseLong(m.group(1));
            int process = Integer.parseInt(m.group(2));
            String key = unquote(m.group(3));
            Operation.OperationType type = Operation.OperationType.valueOf(m.group(4));
            String arg = unquote(m.group(5));
            String returned = unquote(m.group(6));
            long invoke = Long.parseLong(m.group(7));
            long complete = Long.parseLong(m.group(8));
            Operation.Outcome outcome = Operation.Outcome.valueOf(m.group(9));
            ops.add(new Operation(id, process, key, type, arg, returned, invoke, complete, outcome));
        }
        return ops;
    }

    private static String unquote(String raw) {
        if (raw == null || raw.equals("null")) {
            return null;
        }
        String inner = raw.substring(1, raw.length() - 1);
        return inner.replace("\\\"", "\"").replace("\\\\", "\\");
    }
}
