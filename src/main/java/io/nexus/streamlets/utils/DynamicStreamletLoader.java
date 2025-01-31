package io.nexus.streamlets.utils;

import io.nexus.streamlets.Streamlet;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

public class DynamicStreamletLoader {

    private Map<String, Streamlet> functionMap = new HashMap<>();

    public void loadFunctionFromRedis(String functionName, String sourceCode) throws Exception {
        // Step 1: Write source code to a temporary file
        String fileName = functionName + ".java";
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(sourceCode);
        }

        // Step 2: Compile the source code
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        int result = compiler.run(null, null, null, fileName);
        if (result != 0) {
            throw new RuntimeException("Compilation failed.");
        }

        // Step 3: Load the compiled class
        File file = new File(".");
        URL[] urls = {file.toURI().toURL()};
        URLClassLoader classLoader = new URLClassLoader(urls);
        Class<?> clazz = classLoader.loadClass(functionName);

        // Step 4: Instantiate the function object
        Streamlet function = (Streamlet) clazz.getDeclaredConstructor().newInstance();

        // Step 5: Populate the map
        functionMap.put(functionName, function);
    }
}
