package io.nexus.streamlets.metadata;

import java.util.List;

public class Policy {

    private String id;
    private String system;
    private String scope;
    private String stream;
    private List<String> pipeline;
    private List<String> storage;

    public Policy() {
        // Needed for object (de)serialization.
    }

    public Policy(String id, String system, String scope, String stream, List<String> pipeline, List<String> storage) {
        this.id = id;
        this.system = system;
        this.scope = scope;
        this.stream = stream;
        this.pipeline = pipeline;
        this.storage = storage;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public List<String> getPipeline() {
        return pipeline;
    }

    public void setPipeline(List<String> pipeline) {
        this.pipeline = pipeline;
    }

    public List<String> getStorage() {
        return storage;
    }

    public void setStorage(List<String> storage) {
        this.storage = storage;
    }
}
