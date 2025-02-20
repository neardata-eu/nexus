package io.nexus.streamlets.compiler;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

public class InMemoryClassLoader extends URLClassLoader {
    private final Map<String, byte[]> compiledClasses;

    public InMemoryClassLoader(Map<String, byte[]> compiledClasses) {
        super(new URL[0], ClassLoader.getSystemClassLoader());
        this.compiledClasses = compiledClasses;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        byte[] byteCode = compiledClasses.get(name);
        if (byteCode == null) {
            throw new ClassNotFoundException(name);
        }
        return defineClass(name, byteCode, 0, byteCode.length);
    }
}
