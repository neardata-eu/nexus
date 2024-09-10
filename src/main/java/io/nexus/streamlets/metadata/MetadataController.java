package io.nexus.streamlets.metadata;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/metadata")
public class MetadataController {

    private final MetadataService metadataService;

    public MetadataController(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    // CREATE or UPDATE Policy
    @PostMapping("/policy")
    public ResponseEntity<String> savePolicy(@RequestBody Policy policy) {
        try {
            this.metadataService.savePolicy(policy);
            return ResponseEntity.ok("Policy saved successfully");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error saving policy: " + e.getMessage());
        }
    }

    // READ Policy
    @GetMapping("/policy/{id}")
    public ResponseEntity<Policy> getPolicy(@PathVariable("id") String id) {
        try {
            Policy policy = metadataService.getPolicy(id);
            return ResponseEntity.ok(policy);
        } catch (Exception e) {
            return ResponseEntity.status(404).body(null);
        }
    }

    // DELETE Policy
    @DeleteMapping("/policy/{id}")
    public ResponseEntity<String> deletePolicy(@PathVariable String id) {
        this.metadataService.deletePolicy(id);
        return ResponseEntity.ok("Policy deleted successfully");
    }

    // Similarly, implement endpoints for Descriptor
}
