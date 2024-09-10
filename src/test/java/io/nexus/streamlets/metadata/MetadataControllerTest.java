package io.nexus.streamlets.metadata;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

@SpringBootTest
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

        verify(this.metadataService, times(1)).savePolicy(any(Policy.class));  // Verify service method was called once
    }
}
