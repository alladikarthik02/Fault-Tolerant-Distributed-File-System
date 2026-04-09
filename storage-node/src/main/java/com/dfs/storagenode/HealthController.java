package com.dfs.storagenode;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HealthController {

    @Value("${storagenode.id}")
    private String nodeId;

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "nodeId", nodeId);
    }
}