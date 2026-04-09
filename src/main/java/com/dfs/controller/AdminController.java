package com.dfs.controller;

import com.dfs.service.ShardRebuildService;
import com.dfs.storage.StorageNodeClient;
import com.dfs.storage.StorageNodeRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Cluster admin endpoints — only active when running with the 'cluster' profile.
 * Lets you inspect node health and trigger shard rebuilds on demand.
 */
@RestController
@RequestMapping("/api/admin")
@Profile("cluster")
public class AdminController {

    private final StorageNodeRegistry registry;
    private final ShardRebuildService rebuildService;

    @Autowired
    public AdminController(StorageNodeRegistry registry, ShardRebuildService rebuildService) {
        this.registry = registry;
        this.rebuildService = rebuildService;
    }

    @GetMapping("/nodes")
    public List<Map<String, Object>> nodes() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (StorageNodeClient n : registry.all()) {
            out.add(Map.of(
                    "url", n.getBaseUrl(),
                    "healthy", n.isHealthy()
            ));
        }
        return out;
    }

    @PostMapping("/rebuild")
    public ResponseEntity<ShardRebuildService.RebuildReport> rebuild() {
        return ResponseEntity.ok(rebuildService.rebuildMissingShards());
    }
}