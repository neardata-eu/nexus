package io.nexus.streamlets.metadata;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.nexus.configuration.MetadataServiceRunner;

@SpringBootTest(classes = MetadataServiceRunner.class)
public class MetadataControllerTest {

    @Autowired
    private WebApplicationContext context;

    @MockBean
    private MetadataService metadataService;

    private MockMvc mockMvc;

    @Autowired
    public void setupMockMvc() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.context).build();
    }

    // TODO: See what's wrong with this test
    @Test
    public void testCreatePolicy() throws Exception {
        // Given
        Policy policy = new Policy("policy123", "kafka", "myScope", "myStream",
                List.of("edge(s1) | cloud(s2)"), List.of("bucket1", "local_store"));
        ObjectMapper objectMapper = new ObjectMapper();
        String policyJson = objectMapper.writeValueAsString(policy);

        // When & Then
        this.mockMvc.perform(post("/api/metadata/policy")
                .contentType(MediaType.APPLICATION_JSON)
                .content(policyJson))
                .andExpect(status().isOk())
                .andExpect(content().string("Policy saved successfully"));

        verify(this.metadataService, times(1)).savePolicy(any(Policy.class)); // Verify service method was called once
    }
}
