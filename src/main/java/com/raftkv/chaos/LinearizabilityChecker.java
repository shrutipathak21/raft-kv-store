package com.raftkv.chaos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LinearizabilityChecker {

    public record KeyResult(String key, boolean linearizable, int opCount, String explanation) {
    }

    public List<KeyResult> check(List<Operation> history) {
        Map<String, List<Operation>> byKey = history.stream()
                .filter(op -> op.outcome() != Operation.Outcome.FAIL)
                .collect(Collectors.groupingBy(Operation::key));

        List<KeyResult> results = new ArrayList<>();
        for (Map.Entry<String, List<Operation>> entry : byKey.entrySet()) {
            results.add(checkKey(entry.getKey(), entry.getValue()));
        }
        results.sort((a, b) -> a.key().compareTo(b.key()));
        return results;
    }

    public boolean isLinearizable(List<Operation> ops) {
        Map<Long, Operation> byId = new HashMap<>();
        Set<Long> allIds = new HashSet<>();
        for (Operation op : ops) {
            byId.put(op.id(), op);
            allIds.add(op.id());
        }
        return search(byId, allIds, null, new HashSet<>());
    }

    public List<Operation> minimizeFailingWitness(List<Operation> opsForKey) {
        List<Operation> working = new ArrayList<>(opsForKey);
        working.sort(java.util.Comparator.comparingLong(Operation::completeNanos));

        int budget = 4000;
        boolean changed = true;
        while (changed && budget > 0) {
            changed = false;
            for (Operation candidate : new ArrayList<>(working)) {
                if (budget-- <= 0) {
                    break;
                }
                if (candidate.type() == Operation.OperationType.PUT) {
                    boolean stillDependedOn = working.stream()
                            .anyMatch(o -> o != candidate && o.type() == Operation.OperationType.GET
                                    && Objects.equals(o.returnedValue(), candidate.argValue()));
                    if (stillDependedOn) {
                        continue;
                    }
                }
                List<Operation> reduced = new ArrayList<>(working);
                reduced.remove(candidate);
                if (!reduced.isEmpty() && !isLinearizable(reduced)) {
                    working = reduced;
                    changed = true;
                }
            }
        }
        return working;
    }

    private KeyResult checkKey(String key, List<Operation> ops) {
        boolean linearizable = isLinearizable(ops);
        String explanation = linearizable
                ? "found a valid linearization consistent with real-time order"
                : "NO valid linearization exists for the recorded GET results — genuine consistency violation";
        return new KeyResult(key, linearizable, ops.size(), explanation);
    }

    private boolean search(Map<Long, Operation> byId, Set<Long> remaining, String currentValue, Set<String> deadStates) {
        if (remaining.isEmpty()) {
            return true;
        }

        String memoKey = memoKey(remaining, currentValue);
        if (deadStates.contains(memoKey)) {
            return false;
        }

        for (Operation op : minimalCandidates(byId, remaining)) {
            if (op.type() == Operation.OperationType.GET) {
                if (!Objects.equals(op.returnedValue(), currentValue)) {
                    continue;
                }
                if (search(byId, without(remaining, op.id()), currentValue, deadStates)) {
                    return true;
                }
            } else {
                Set<Long> next = without(remaining, op.id());
                if (op.outcome() == Operation.Outcome.OK) {
                    if (search(byId, next, op.argValue(), deadStates)) {
                        return true;
                    }
                } else {
                    if (search(byId, next, op.argValue(), deadStates)) {
                        return true;
                    }
                    if (search(byId, next, currentValue, deadStates)) {
                        return true;
                    }
                }
            }
        }

        deadStates.add(memoKey);
        return false;
    }

    private List<Operation> minimalCandidates(Map<Long, Operation> byId, Set<Long> remaining) {
        List<Operation> result = new ArrayList<>();
        candidateLoop:
        for (Long id : remaining) {
            Operation op = byId.get(id);
            for (Long otherId : remaining) {
                if (otherId.equals(id)) {
                    continue;
                }
                Operation other = byId.get(otherId);
                if (other.completeNanos() <= op.invokeNanos()) {
                    continue candidateLoop;
                }
            }
            result.add(op);
        }
        return result;
    }

    private Set<Long> without(Set<Long> set, long id) {
        Set<Long> copy = new HashSet<>(set);
        copy.remove(id);
        return copy;
    }

    private String memoKey(Set<Long> remaining, String currentValue) {
        List<Long> sorted = new ArrayList<>(remaining);
        sorted.sort(Long::compareTo);
        return sorted + "|" + currentValue;
    }
}
