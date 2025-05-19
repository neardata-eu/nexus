package io.nexus.streamlets.functions;

import io.nexus.streamlets.deserializers.KafkaImageDeserializer;

public class KafkaImageClassificationStreamlet extends ImageClassificationStreamlet {

    public KafkaImageClassificationStreamlet(KafkaImageDeserializer deserializer) {
        super(deserializer);
    }
}
