package com.dfs.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;

import java.io.IOException;

/**
 * Remote-backed shard store that talks to storage nodes over HTTP.
 *
 * Marked @Primary so Spring injects this instead of the local-disk ShardStore
 * wherever ShardStore is autowired. The local ShardStore class is kept for tests
 * but the live data path runs through here.
 *
 * Reads use a "primary first, then fallback" strategy: try the node that should
 * have the shard according to the placement rule; if it's down or 404s, try the
 * other nodes too in case the shard was rebuilt elsewhere.
 */
@Component
@Primary
@Profile("cluster")
public class RemoteShardStore extends ShardStore {

    private static final Logger log = LoggerFactory.getLogger(RemoteShardStore.class);

    private final StorageNodeRegistry registry;

    public RemoteShardStore(StorageNodeRegistry registry) {
        this.registry = registry;
    }

    /** Override the local-disk init from the parent so it doesn't try to mkdir. */
    @Override
    public void init() {
        log.info("RemoteShardStore initialized with {} storage nodes", registry.size());
    }

    @Override
    public void writeShard(String chunkId, int shardIndex, byte[] data) throws IOException {
        StorageNodeClient primary = registry.primaryFor(shardIndex);
        try {
            primary.putShard(chunkId, shardIndex, data);
        } catch (Exception e) {
            // Fallback: try any healthy node we can find
            for (StorageNodeClient n : registry.all()) {
                if (n == primary) continue;
                try {
                    n.putShard(chunkId, shardIndex, data);
                    log.warn("Primary node {} unreachable for shard {}/{}; wrote to fallback {}",
                            primary.getBaseUrl(), chunkId, shardIndex, n.getBaseUrl());
                    return;
                } catch (Exception ignored) {}
            }
            throw new IOException("No healthy storage nodes accepted shard " + chunkId + "/" + shardIndex, e);
        }
    }

    @Override
    public byte[] readShard(String chunkId, int shardIndex) throws IOException {
        StorageNodeClient primary = registry.primaryFor(shardIndex);
        // Try primary first
        try {
            byte[] data = primary.getShard(chunkId, shardIndex);
            if (data != null) return data;
        } catch (Exception e) {
            log.debug("Primary {} unreachable for shard {}/{}, trying fallbacks",
                    primary.getBaseUrl(), chunkId, shardIndex);
        }
        // Fallback: scan all other nodes (in case the shard was rebuilt elsewhere)
        for (StorageNodeClient n : registry.all()) {
            if (n == primary) continue;
            try {
                byte[] data = n.getShard(chunkId, shardIndex);
                if (data != null) return data;
            } catch (Exception ignored) {}
        }
        return null; // tells ErasureCodingService this shard is unavailable
    }

    @Override
    public boolean shardExists(String chunkId, int shardIndex) {
        try {
            return readShard(chunkId, shardIndex) != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean deleteShard(String chunkId, int shardIndex) throws IOException {
        boolean any = false;
        for (StorageNodeClient n : registry.all()) {
            if (n.deleteShard(chunkId, shardIndex)) any = true;
        }
        return any;
    }
}