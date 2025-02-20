package io.nexus.streamlets.functions;

import io.nexus.streamlets.EventStreamlet;
import io.nexus.streamlets.context.StreamletContext;
import io.nexus.streamlets.deserializers.StringDeserializer;

import java.util.regex.Pattern;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Streamlet that counts the number of words in deserialized string records.
 */
public class WordCountStreamlet extends EventStreamlet<String> {
    private final AtomicLong wordCount = new AtomicLong(0);
    private static final Pattern WORD_PATTERN = Pattern.compile("\\s+");

    public WordCountStreamlet(StringDeserializer deserializer) {
        super(deserializer);
    }

    @Override
    protected void processPutRecord(String record, StreamletContext context) {
        wordCount.addAndGet(countWords(record));
        context.getLogger().info("PUT - Executing Streamlet: WORDCOUNT {}, as part of pipeline: {}", wordCount.get(),
                context.getPolicy().getPipeline());
    }

    @Override
    protected void processGetRecord(String record, StreamletContext context) {
        wordCount.addAndGet(countWords(record));
        context.getLogger().info("GET - Executing Streamlet: WORDCOUNT {}, as part of pipeline: {}", wordCount.get(),
                context.getPolicy().getPipeline());
    }

    private long countWords(String record) {
        return WORD_PATTERN.splitAsStream(record)
                .filter(word -> !word.isEmpty())
                .count();
    }
}
