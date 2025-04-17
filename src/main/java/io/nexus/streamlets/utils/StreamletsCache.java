package io.nexus.streamlets.utils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.nexus.streamlets.Streamlet;
import io.nexus.streamlets.compiler.StreamletLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StreamletsCache {
    final Logger logger = LoggerFactory.getLogger(StreamletsCache.class);
    private final LoadingCache<StreamletCacheKey, Streamlet> cache;

    public StreamletsCache(int maxSize, long expirationTime, TimeUnit expirationUnit, StreamletLoader streamletLoader) {
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(expirationTime, expirationUnit)
                .build(new CacheLoader<StreamletCacheKey, Streamlet>() {
                    @Override
                    public Streamlet load(StreamletCacheKey key) throws Exception {
                        logger.info("Loading new Streamlet {}.", key.getStreamletId());
                        return streamletLoader.createStreamlet(key.getStreamletId());
                    }
                });
    }

    public boolean exists(String streamPartition, String streamletId) {
        StreamletCacheKey key = new StreamletCacheKey(streamPartition, streamletId);
        return cache.getIfPresent(key) != null;
    }

    public Streamlet getOrLoadStreamlet(String streamPartition, String streamletId) {
        StreamletCacheKey key = new StreamletCacheKey(streamPartition, streamletId);
        return cache.getUnchecked(key);
    }

    private static class StreamletCacheKey {
        private final String streamPartition;
        private final String streamletId;

        public StreamletCacheKey(String streamPartition, String streamletId) {
            this.streamPartition = streamPartition;
            this.streamletId = streamletId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamletCacheKey that = (StreamletCacheKey) o;
            return streamPartition.equals(that.streamPartition) && streamletId.equals(that.streamletId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(streamPartition, streamletId);
        }

        public String getStreamPartition() {
            return streamPartition;
        }

        public String getStreamletId() {
            return streamletId;
        }
    }
}