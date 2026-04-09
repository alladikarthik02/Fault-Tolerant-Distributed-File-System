package com.dfs.storage;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Local on-disk shard store. Each shard lives at {dfs.shard.root}/{chunkId}/{shardIndex}.
 *
 * This is the base implementation used in unit tests and as a fallback. In the
 * running cluster, RemoteShardStore (which extends this class) is marked @Primary
 * and replaces it — all reads/writes go over HTTP to storage nodes instead.
 */
@Component
public class ShardStore {

    private static final Logger log = LoggerFactory.getLogger(ShardStore.class);

    @Value("${dfs.shard.root:./data/shards}")
    private String shardRoot;

    private Path rootPath;

    @PostConstruct
    public void init() throws IOException {
        rootPath = Paths.get(shardRoot).toAbsolutePath().normalize();
        Files.createDirectories(rootPath);
        log.info("ShardStore initialized at {}", rootPath);
    }

    public void writeShard(String chunkId, int shardIndex, byte[] data) throws IOException {
        Path dir = rootPath.resolve(chunkId);
        Files.createDirectories(dir);
        Files.write(dir.resolve(String.valueOf(shardIndex)), data);
    }

    /** Returns null if the shard is missing — callers must handle this for EC recovery. */
    public byte[] readShard(String chunkId, int shardIndex) throws IOException {
        Path file = rootPath.resolve(chunkId).resolve(String.valueOf(shardIndex));
        if (!Files.exists(file)) {
            return null;
        }
        return Files.readAllBytes(file);
    }

    public boolean shardExists(String chunkId, int shardIndex) {
        return Files.exists(rootPath.resolve(chunkId).resolve(String.valueOf(shardIndex)));
    }

    public boolean deleteShard(String chunkId, int shardIndex) throws IOException {
        return Files.deleteIfExists(rootPath.resolve(chunkId).resolve(String.valueOf(shardIndex)));
    }

    public Path getRootPath() {
        return rootPath;
    }
}