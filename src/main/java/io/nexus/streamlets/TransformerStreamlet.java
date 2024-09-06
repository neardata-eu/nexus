package io.nexus.streamlets;

import io.nexus.streamlets.utils.ByteBufferPipelineStream;

public interface TransformerStreamlet extends Streamlet {

    public void doTransform(ByteBufferPipelineStream input, ByteBufferPipelineStream output);

    // TODO
    //public InputStream processGet(DynamicInputStream input);

    // TODO
    //public InputStream processList(DynamicInputStream input);

}
