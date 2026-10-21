package dev.conductor.core;

import java.util.*;

/** A fenced task lease. Owner + fence + live lease must all match for a write to be accepted. */
public record Claim(String execution,String task,String owner,long fence,long attempt,
                    String idempotencyKey,Map<String,Object> spec,long readyAt) {
    public Map<String,Object> toWire() {
        Map<String,Object> m=new LinkedHashMap<>();
        m.put("execution",execution);m.put("task",task);m.put("owner",owner);m.put("fence",fence);
        m.put("attempt",attempt);m.put("idempotencyKey",idempotencyKey);m.put("spec",spec);m.put("readyAt",readyAt);
        return m;
    }
    public static Claim fromWire(Map<String,Object> m) {
        return new Claim(Json.str(m,"execution",""),Json.str(m,"task",""),Json.str(m,"owner",""),
                Json.num(m,"fence",0),Json.num(m,"attempt",0),Json.str(m,"idempotencyKey",""),
                Json.obj(m.get("spec")),Json.num(m,"readyAt",0));
    }
}
