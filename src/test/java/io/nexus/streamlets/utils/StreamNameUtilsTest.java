package io.nexus.streamlets.utils;

import static org.junit.Assert.*;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.junit.Test;
import org.mockito.Mockito;

public class StreamNameUtilsTest {

    @Test
    public void testGetScopeFromChunkName_Kafka() {
        String mockChunkName = "topic1-NocMJpqTTtyKaFJDURStjg/0/00000000000000000000-zsj9DCIER9OaE0B3ZwcMpQ.log";
        assertEquals("topic1", StreamNameUtils.getScopeFromChunkName(mockChunkName));
    }

    @Test
    public void testGetStreamFromChunkName_Kafka() {
        String mockChunkName = "topic1-NocMJpqTTtyKaFJDURStjg/0/00000000000000000000-zsj9DCIER9OaE0B3ZwcMpQ.log";
        assertEquals("0", StreamNameUtils.getStreamFromChunkName(mockChunkName));
    }

    @Test
    public void testGetScopeFromChunkName_Pulsar() {
        String mockChunkName = "d536f64d-4a32-43e5-944e-b32e05e3c790-ledger-16";
        assertEquals("pulsar", StreamNameUtils.getScopeFromChunkName(mockChunkName));
    }

    @Test
    public void testGetStreamFromChunkName_Pulsar() {
        String mockChunkName = "d536f64d-4a32-43e5-944e-b32e05e3c790-ledger-16";
        assertEquals("pulsar", StreamNameUtils.getStreamFromChunkName(mockChunkName));
    }

    @Test
    public void testGetScopeFromChunkName_Default() {
        String mockChunkName = "scope/stream/test.txt";
        assertEquals("scope", StreamNameUtils.getScopeFromChunkName(mockChunkName));
    }

    @Test
    public void testGetStreamFromChunkName_Default() {
        String mockChunkName = "scope/stream/test.txt";
        assertEquals("stream", StreamNameUtils.getStreamFromChunkName(mockChunkName));
    }

    @Test
    public void testGetScopeFromRequest_MultipartUpload() {
        MultipartUpload multipartUpload = Mockito.mock(MultipartUpload.class);
        Mockito.when(multipartUpload.blobName())
                .thenReturn("topic1-NocMJpqTTtyKaFJDURStjg/0/00000000000000000000-zsj9DCIER9OaE0B3ZwcMpQ.log");
        assertEquals("topic1", StreamNameUtils.getScopeFromRequest(multipartUpload));
    }

    @Test
    public void testGetStreamFromRequest_MultipartUpload() {
        MultipartUpload multipartUpload = Mockito.mock(MultipartUpload.class);
        Mockito.when(multipartUpload.blobName())
                .thenReturn("topic1-NocMJpqTTtyKaFJDURStjg/0/00000000000000000000-zsj9DCIER9OaE0B3ZwcMpQ.log");
        assertEquals("0", StreamNameUtils.getStreamFromRequest(multipartUpload));
    }
}