package io.nexus.streamlets;

import io.nexus.streamlets.utils.DynamicInputStream;

import java.io.InputStream;

public interface Streamlet {

    public InputStream processPut(DynamicInputStream input);

    // TODO
    //public InputStream processGet(DynamicInputStream input);

    // TODO
    //public InputStream processList(DynamicInputStream input);
}
