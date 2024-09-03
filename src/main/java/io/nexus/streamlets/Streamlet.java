package io.nexus.streamlets;

import io.nexus.streamlets.utils.ByteBufferPipelineStream;

import java.io.InputStream;

public interface Streamlet {

    public void processPut(ByteBufferPipelineStream input, ByteBufferPipelineStream output);

    // TODO
    //public InputStream processGet(DynamicInputStream input);

    // TODO
    //public InputStream processList(DynamicInputStream input);
}
