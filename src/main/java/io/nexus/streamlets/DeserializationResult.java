package io.nexus.streamlets;

import java.util.List;

public class DeserializationResult<T> {
    private final List<T> records;
    private final int bytesConsumed;

    public DeserializationResult(List<T> records, int bytesConsumed) {
        this.records = records;
        this.bytesConsumed = bytesConsumed;
    }

    public List<T> records() {
        return records;
    }

    public int bytesConsumed() {
        return bytesConsumed;
    }
}