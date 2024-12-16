package io.nexus.streamlets;

import io.nexus.streamlets.utils.ByteBufferPipelineStream;

public interface Streamlet {
    // TODO: Abstract time measurement for doPut and doGet

    // Each subclass should provide an implementation on how that subclass's
    // streamlets should operate/process data when there is a read or write

    // Invoked when there is a PUT/write request
    public void doPut(ByteBufferPipelineStream input, ByteBufferPipelineStream output);

    // Invoked when there is a GET/read request
    public void doGet(ByteBufferPipelineStream input, ByteBufferPipelineStream output);

}
