package io.nexus.streamlets.utils;

//A record encapsulating the stream-based input for streamlets
//TODO: Will be revisited during event-based implementation
public record InputStreamRecord(ByteBufferPipelineStream input, ByteBufferPipelineStream output) {
}