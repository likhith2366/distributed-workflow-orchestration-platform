package dev.conductor.core;

import java.util.*;

/** Built-in demonstration activities. Real deployments replace these with idempotent service calls. */
public final class Activities {
    private Activities() {}
    public static Object execute(Claim c,EffectLedger ledger)throws Exception {
        Map<String,Object> spec=c.spec();
        if(c.attempt()<=Json.num(spec,"failUntil",0))throw new IllegalStateException("Injected transient failure");
        // Simulated downstream service time, so worker-pool scaling can be measured.
        long latency=Json.num(spec,"latencyMs",0);
        if(latency>0)Thread.sleep(latency);
        return switch(Json.str(spec,"action","echo")) {
            case "fail" -> throw new IllegalStateException("Declared permanent failure");
            case "sleep" -> {Thread.sleep(Json.num(spec,"durationMs",0));yield Map.of("sleptMs",Json.num(spec,"durationMs",0));}
            case "ledger" -> ledger.apply(c.idempotencyKey(),effectValue(c));
            default -> spec.getOrDefault("value",Map.of("ok",true));
        };
    }
    public static Object effectValue(Claim c){return c.spec().getOrDefault("value",Map.of("recorded",true));}
}
