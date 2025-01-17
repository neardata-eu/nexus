package io.nexus.streamlets.state;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Data structures in Streamlets annotated with this annotation will
 * be persistently stored across executions. The name parameter in the
 * annotation allows the system to locate the contents of the data
 * in the selected storage backend.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Persistent {
    String name(); // The name of the persistent data structure
    StatePersistenceType type(); // Whether the variable is supposed to be shared across multiple functions or not
}
