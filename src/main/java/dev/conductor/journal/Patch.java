package dev.conductor.journal;

import dev.conductor.core.*;
import java.util.*;

/**
 * Field-level delta between two consecutive projections of one execution, so the WAL stores what a
 * transition changed instead of the whole execution (definition, every step spec, every result).
 *
 * <pre>{"run": {changed top-level fields}, "tasks": {id: {changed fields}}, "added": {id: full task}}</pre>
 *
 * Transitions only add or overwrite keys and append tasks, and {@link LinkedHashMap} keeps an
 * overwritten key in place and appends a new one, so applying a patch in the new projection's
 * iteration order reproduces it byte for byte. Anything else (a removed key or task) yields
 * {@code null}, and the caller writes a full record instead.
 */
final class Patch {
    private Patch() {}

    static Map<String,Object> diff(Map<String,Object> before,Map<String,Object> after) {
        for(String k:before.keySet())if(!after.containsKey(k))return null;
        Map<String,Object> run=new LinkedHashMap<>();
        for(var e:after.entrySet()) {
            if(e.getKey().equals("tasks"))continue;
            if(!before.containsKey(e.getKey())||!Objects.equals(before.get(e.getKey()),e.getValue()))run.put(e.getKey(),e.getValue());
        }
        Map<String,Object> oldTasks=Workflow.tasks(before),newTasks=Workflow.tasks(after);
        for(String id:oldTasks.keySet())if(!newTasks.containsKey(id))return null;
        Map<String,Object> changed=new LinkedHashMap<>(),added=new LinkedHashMap<>();
        for(var e:newTasks.entrySet()) {
            Object previous=oldTasks.get(e.getKey());
            if(previous==null){added.put(e.getKey(),e.getValue());continue;}
            if(previous==e.getValue())continue;
            Map<String,Object> o=Json.obj(previous),n=Json.obj(e.getValue());
            for(String k:o.keySet())if(!n.containsKey(k))return null;
            Map<String,Object> fields=new LinkedHashMap<>();
            // Identity first: shared nested values (spec, results) are the same object when unchanged.
            for(var f:n.entrySet())if(!o.containsKey(f.getKey())||o.get(f.getKey())!=f.getValue()&&!Objects.equals(o.get(f.getKey()),f.getValue()))
                fields.put(f.getKey(),f.getValue());
            if(!fields.isEmpty())changed.put(e.getKey(),fields);
        }
        Map<String,Object> patch=new LinkedHashMap<>();
        if(!run.isEmpty())patch.put("run",run);
        if(!changed.isEmpty())patch.put("tasks",changed);
        if(!added.isEmpty())patch.put("added",added);
        return patch;
    }

    static Map<String,Object> apply(Map<String,Object> before,Map<String,Object> patch) {
        Map<String,Object> after=Workflow.mutableCopy(before);
        Map<String,Object> tasks=Workflow.tasks(after);
        if(patch.containsKey("run"))after.putAll(Json.obj(patch.get("run")));
        if(patch.containsKey("tasks"))for(var e:Json.obj(patch.get("tasks")).entrySet()) {
            Object task=tasks.get(e.getKey());
            if(task==null)throw new IllegalStateException("Patch names unknown task "+e.getKey());
            Json.obj(task).putAll(Json.obj(e.getValue()));
        }
        if(patch.containsKey("added"))tasks.putAll(Json.obj(patch.get("added")));
        return after;
    }
}
