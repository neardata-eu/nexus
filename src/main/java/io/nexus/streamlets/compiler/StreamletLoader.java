package io.nexus.streamlets.compiler;

import io.nexus.streamlets.*;
import io.nexus.streamlets.metadata.MetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;

public class StreamletLoader {
    final Logger logger = LoggerFactory.getLogger(StreamletLoader.class);
    private static final String STREAMLETS_PACKAGE = "io.nexus.streamlets.functions";
    private static final String DESERIALIZERS_PACKAGE = "io.nexus.streamlets.deserializers";

    private final MetadataService metadataService;
    private final DynamicCompiler compiler;
    private final Map<String, Class<?>> loadedClasses = new ConcurrentHashMap<>();

    public StreamletLoader(MetadataService metadataService) {
        this.metadataService = metadataService;
        this.compiler = new DynamicCompiler();
        preloadLocalClasses(STREAMLETS_PACKAGE);
        preloadLocalClasses(DESERIALIZERS_PACKAGE);
        logger.info("Loaded local Streamlet and Deserializer code {}.", loadedClasses.size());
    }

    // Instance creation region

    public Streamlet createStreamlet(String name) {
        try {
            long startTime = System.nanoTime();
            Class<?> streamletClass = loadClass(name);
            // Check if the class is an EventStreamlet by looking for a constructor that takes a Deserializer
            for (Constructor<?> constructor : streamletClass.getDeclaredConstructors()) {
                if (constructor.getParameterCount() == 1 && Deserializer.class.isAssignableFrom(constructor.getParameterTypes()[0])) {
                    Class<?> deserializerClass = loadClass(constructor.getParameterTypes()[0].getName());
                    Deserializer<?> deserializer = (Deserializer<?>) deserializerClass.getDeclaredConstructor().newInstance();
                    Streamlet s = (EventStreamlet<?>) constructor.newInstance(deserializer);
                    StreamletsMetrics.STREAMLET_INSTANTIATION_DURATION_TIMER.record(System.nanoTime() - startTime);
                    return s;
                }
            }
            // Otherwise, assume it's a ByteStreamlet
            Streamlet s = (ByteStreamlet) streamletClass.getDeclaredConstructor().newInstance();
            StreamletsMetrics.STREAMLET_INSTANTIATION_DURATION_TIMER.record(System.nanoTime() - startTime);
            return s;
        } catch(Exception ex) {
            logger.error("Problem creating streamlet.", ex);
            throw new RuntimeException(ex);
        }
    }

    // end region

    // Class loader region

    private Class<?> loadClass(String name) throws Exception {
        if (loadedClasses.containsKey(name)) {
            return loadedClasses.get(name);
        }

        String code = metadataService.getStreamletCode(name);
        if (code == null) {
            throw new IllegalArgumentException("Streamlet/deserializer " + name + " not found in Redis.");
        }

        if (!compiler.compile(name, code)) {
            throw new RuntimeException("Compilation failed for " + name);
        }

        Class<?> clazz = compiler.loadClass(name);
        loadedClasses.put(name, clazz);
        return clazz;
    }

    /**
     * Scans and preloads all classes from the given package.
     */
    private void preloadLocalClasses(String packageName) {
        try {
            String path = packageName.replace('.', '/');
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(path);

            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                if (resource.getProtocol().equals("file")) {
                    // Running in an IDE, load from file system
                    Path directory = Paths.get(resource.toURI());
                    if (Files.isDirectory(directory)) {
                        Files.walk(directory)
                                .filter(Files::isRegularFile)
                                .filter(file -> file.toString().endsWith(".class"))
                                .forEach(file -> loadClassFromFile(file, packageName, directory));
                    }
                } else if (resource.getProtocol().equals("jar")) {
                    // Running from a JAR, extract class names differently
                    String jarPath = resource.getPath().substring(5, resource.getPath().indexOf("!"));
                    try (JarFile jarFile = new JarFile(jarPath)) {
                        jarFile.stream()
                                .filter(entry -> entry.getName().endsWith(".class") && entry.getName().startsWith(path))
                                .forEach(entry -> {
                                    String className = entry.getName()
                                            .replace('/', '.')
                                            .replace(".class", "");
                                    try {
                                        Class<?> clazz = Class.forName(className);
                                        loadedClasses.put(clazz.getName(), clazz);
                                    } catch (ClassNotFoundException e) {
                                        logger.warn("Could not load class from JAR: {}", className);
                                    }
                                });
                    }
                }
            }
        } catch (IOException | URISyntaxException e) {
            logger.error("Error loading classes from package: {}", packageName, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads a class from a compiled `.class` file.
     */
    private void loadClassFromFile(Path file, String packageName, Path baseDir) {
        try {
            String className = packageName + "." + baseDir.relativize(file).toString()
                    .replace(".class", "")
                    .replace("/", ".");
            Class<?> clazz = Class.forName(className);
            loadedClasses.put(clazz.getName(), clazz);
        } catch (ClassNotFoundException e) {
            logger.error("Could not load class: {}", file, e);
            throw new RuntimeException(e);
        }
    }

    // end region
}
