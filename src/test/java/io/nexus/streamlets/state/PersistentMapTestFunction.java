package io.nexus.streamlets.state;

import java.util.HashMap;
import java.util.function.BiFunction;

public class PersistentMapTestFunction implements BiFunction<String, String, String> {

    @Persistent(name = "persistentMap", type = StatePersistenceType.SHARED)
    private final HashMap<String, String> persistentMap = new HashMap<>();

    @Override
    public String apply(String a, String b) {
        return persistentMap.put(a, b);
    }
}
