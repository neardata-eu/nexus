package io.nexus.streamlets.metadata;

import redis.clients.jedis.Jedis;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedisConfig {

    @Bean
    public Jedis jedis() {
        return new Jedis("localhost", 6379);  // Replace with your Redis host and port
    }
}
