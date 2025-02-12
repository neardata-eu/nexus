package io.nexus.streamlets.deserializers;

import io.nexus.streamlets.Deserializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class StringDeserializer implements Deserializer<String> {
    @Override
    public String deserialize(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        return reader.readLine(); // Read one line at a time
    }
}