package io.nexus.streamlets.state;

import io.nexus.streamlets.state.backends.RocksDBStateBackend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;

public class FunctionStateTestRocksDB extends AbstractFunctionStateTest {

    private final String rocksDBPath = "/tmp/nexus";

    @BeforeEach
    void setup() {
        this.backend = new RocksDBStateBackend(rocksDBPath);
        this.manager = new StreamletStateManager(backend);
    }

    @AfterEach
    void tearDown() {
        deleteDirectoryIfExists(new File(rocksDBPath));
    }

    public static void deleteDirectoryIfExists(File directory) {
        if (directory.exists()) {
            deleteDirectory(directory);
            System.out.println("Directory deleted: " + directory.getAbsolutePath());
        } else {
            System.out.println("Directory does not exist: " + directory.getAbsolutePath());
        }
    }

    private static void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) { // Check if directory is not empty
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file); // Recursive call for subdirectories
                } else {
                    file.delete();
                }
            }
        }
        directory.delete(); // Delete the empty directory itself
    }
}
