package io.nexus.streamlets.metadata;

public interface MetadataCallback {
    void onMessage(String key, String message);
}
