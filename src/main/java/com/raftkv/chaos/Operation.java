package com.raftkv.chaos;

public record Operation(long id, int processId, String key, OperationType type, String argValue,
                         String returnedValue, long invokeNanos, long completeNanos, Outcome outcome) {

    public enum OperationType { PUT, GET }

    public enum Outcome { OK, FAIL, INDETERMINATE }
}
