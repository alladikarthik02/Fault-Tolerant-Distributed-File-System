package com.dfs.service;

import com.dfs.erasure.ErasureCodingService;
import com.dfs.model.Chunk;
import com.dfs.model.FileMetadata;
import com.dfs.storage.RemoteShardStore;
import com.dfs.storage.StorageNodeClient;
import com.dfs.storage.StorageNodeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * Detects down storage nodes and rebuilds their shards via Reed-Solomon.
 *
 * Strategy on rebuild:
 *   1. Find all nodes currently not healthy.
 *   2. For every known file in the metadata store, for every chunk in that file,
 *      for every shard index, check whether the primary node (the one we expect
 *      to hold it) is one of the down nodes.
 *   3. If so, trigger a read-and-decode on the chunk via ErasureCodingService.
 *      That already tolerates missing shards up to the parity count.
 *   4. Re-encode and write the missing shards to a healthy fallback node.
 *
 * This is invoked manually via the admin endpoint for now. A scheduled task
 * could run it every N seconds, but on-demand is cleaner for a demo.
 */
@Service
@Profile("cluster")
public class ShardRebuildService {

    private static final Logger log = LoggerFactory.getLogger(ShardRebuildService.class);

    private final MetadataStore metadata;
    private final StorageNodeRegistry registry;
    private final ErasureCodingService ec;
    private final RemoteShardStore shardStore;

    public ShardRebuildService(MetadataStore metadata,
                               StorageNodeRegistry registry,
                               ErasureCodingService ec,
                               RemoteShardStore shardStore) {
        this.metadata = metadata;
        this.registry = registry;
        this.ec = ec;
        this.shardStore = shardStore;
    }

    /**
     * Scan every known file and rebuild any shard that lives on a currently-unhealthy node.
     * Returns a summary of how many shards were rebuilt.
     */
    public RebuildReport rebuildMissingShards() {
    long start = System.currentTimeMillis();
    int chunksProcessed = 0;
    int chunksRebuilt = 0;
    int shardsRebuilt = 0;
    int errors = 0;

    boolean[] nodeHealthy = new boolean[registry.size()];
    for (int i = 0; i < registry.size(); i++) {
        nodeHealthy[i] = registry.all().get(i).isHealthy();
    }
    log.info("Rebuild scan: node health = {}", describeHealth(nodeHealthy));

    // Count how many shards have their primary on a down node
    int totalShards = ec.getTotalShards();
    int shardsPerChunkNeedingRebuild = 0;
    for (int s = 0; s < totalShards; s++) {
        if (!nodeHealthy[s % registry.size()]) shardsPerChunkNeedingRebuild++;
    }

    if (shardsPerChunkNeedingRebuild == 0) {
        log.info("All nodes healthy, nothing to rebuild");
        return new RebuildReport(0, 0, 0, 0, System.currentTimeMillis() - start);
    }

    for (FileMetadata meta : metadata.list()) {
        for (Chunk chunk : meta.chunks()) {
            chunksProcessed++;
            try {
                byte[] chunkData = ec.readAndDecode(chunk.chunkId());
                // Re-encode: RemoteShardStore will write each shard to its primary if up,
                // or fall back to a healthy node if down. Missing shards on down nodes
                // thus get recreated on surviving nodes automatically.
                ec.encodeAndStore(chunk.chunkId(), chunkData);
                chunksRebuilt++;
                shardsRebuilt += shardsPerChunkNeedingRebuild;
            } catch (IOException e) {
                log.error("Rebuild failed for chunk {}: {}", chunk.chunkId(), e.getMessage());
                errors++;
            }
        }
    }

    long durationMs = System.currentTimeMillis() - start;
    RebuildReport report = new RebuildReport(chunksProcessed, chunksRebuilt, shardsRebuilt, errors, durationMs);
    log.info("Rebuild complete: {}", report);
    return report;
}

    private String describeHealth(boolean[] health) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < health.length; i++) {
            if (i > 0) sb.append(", ");
            StorageNodeClient n = registry.all().get(i);
            sb.append(n.getBaseUrl()).append("=").append(health[i] ? "UP" : "DOWN");
        }
        return sb.append("]").toString();
    }

    public record RebuildReport(
            int chunksProcessed,
            int chunksRebuilt,
            int shardsRebuilt,
            int errors,
            long durationMs
    ) {}
}