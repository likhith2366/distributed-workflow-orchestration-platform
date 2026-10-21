package dev.conductor.core;

import java.io.IOException;

/**
 * Example downstream system that atomically stores an idempotency key with its effect.
 * Repeating a key returns the original result; a different payload for the same key is rejected.
 */
public interface EffectLedger extends AutoCloseable {
    Object apply(String key,Object value)throws IOException;
    long size()throws IOException;
    @Override void close()throws IOException;
}
