package io.nexus.streamlets.state;

import io.nexus.streamlets.state.backends.InMemoryStateBackend;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FunctionStateTestInMemory extends AbstractFunctionStateTest {

    @BeforeEach
    void setup() {
        this.backend = new InMemoryStateBackend();
        this.manager = new StreamletStateManager(backend);
    }

}
