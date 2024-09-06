package io.nexus;

import io.nexus.s3proxy.S3Proxy;
import io.nexus.streamlets.StreamletsInterception;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;

import java.net.URI;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("jclouds.filesystem.basedir", "/tmp/blobstore");

        BlobStoreContext context = ContextBuilder
                .newBuilder("filesystem")
                .credentials("identity", "credential")
                .overrides(properties)
                .build(BlobStoreContext.class);

        StreamletsInterception streamletsMiddleware =  new StreamletsInterception(context.getBlobStore());

        S3Proxy s3Proxy = S3Proxy.builder()
                .blobStore(streamletsMiddleware)
                .endpoint(URI.create("http://127.0.0.1:8080"))
                .build();

        s3Proxy.start();
        while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
            Thread.sleep(1);
        }
    }
}
