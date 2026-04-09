package com.dfs.storage;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.context.annotation.Profile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Holds the cluster topology — the list of storage nodes the controller can talk to.
 * Decides shard placement using simple round-robin (rack-aware in spirit:
 * with 3 nodes and shards 0..8, every node gets exactly 3 shards per chunk,
 * so any single-node failure leaves 6 surviving shards = enough to recover).
 */
@Component
@Profile("cluster")
public class StorageNodeRegistry {

    private static final Logger log = LoggerFactory.getLogger(StorageNodeRegistry.class);

    @Value("${dfs.nodes}")
    private String nodesCsv;

    private final RestTemplate http;
    private List<StorageNodeClient> nodes;

    public StorageNodeRegistry(RestTemplate http) {
        this.http = http;
    }

    @PostConstruct
    public void init() {
        nodes = new ArrayList<>();
        for (String url : Arrays.stream(nodesCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList()) {
            nodes.add(new StorageNodeClient(url, http));
        }
        log.info("StorageNodeRegistry initialized with {} nodes: {}", nodes.size(), nodesCsv);
    }

    public List<StorageNodeClient> all() {
        return nodes;
    }

    public int size() {
        return nodes.size();
    }

    /**
     * Primary placement: shard `shardIndex` lives on node `shardIndex % N`.
     * With 9 shards and 3 nodes, each node gets exactly 3 shards per chunk.
     * Critically: with 6+3 Reed-Solomon, losing one whole node leaves 6 shards = recoverable.
     */
    public StorageNodeClient primaryFor(int shardIndex) {
        return nodes.get(shardIndex % nodes.size());
    }

    public List<StorageNodeClient> healthyNodes() {
        return nodes.stream().filter(StorageNodeClient::isHealthy).toList();
    }
}