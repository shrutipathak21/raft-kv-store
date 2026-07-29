package com.raftkv.chaos;

import java.util.List;

public final class ReplayChecker {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ReplayChecker <historyJsonPath> [keyFilter]");
            System.exit(1);
        }
        List<Operation> history = HistoryIO.load(args[0]);
        String keyFilter = args.length > 1 ? args[1] : null;
        if (keyFilter != null) {
            history = history.stream().filter(o -> o.key().equals(keyFilter)).toList();
        }

        System.out.println("Loaded " + history.size() + " operations from " + args[0]);
        LinearizabilityChecker checker = new LinearizabilityChecker();
        List<LinearizabilityChecker.KeyResult> results = checker.check(history);

        boolean allOk = true;
        for (LinearizabilityChecker.KeyResult r : results) {
            System.out.println((r.linearizable() ? "PASS" : "FAIL") + "  key=" + r.key()
                    + "  (" + r.opCount() + " ops)  " + r.explanation());
            if (!r.linearizable()) {
                allOk = false;
                List<Operation> opsForKey = history.stream()
                        .filter(op -> op.key().equals(r.key()) && op.outcome() != Operation.Outcome.FAIL)
                        .toList();
                List<Operation> witness = checker.minimizeFailingWitness(opsForKey);
                System.out.println("  Minimal counterexample (" + witness.size() + " ops):");
                for (Operation op : witness) {
                    System.out.println(String.format(
                            "    id=%-4d proc=%-2d %-3s arg=%-8s ret=%-8s outcome=%-13s invoke=%5dms complete=%5dms",
                            op.id(), op.processId(), op.type(), op.argValue(), op.returnedValue(), op.outcome(),
                            op.invokeNanos() / 1_000_000, op.completeNanos() / 1_000_000));
                }
            }
        }
        System.exit(allOk ? 0 : 1);
    }
}
