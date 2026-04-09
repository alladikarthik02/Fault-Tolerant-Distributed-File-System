package com.dfs.storage;

import com.dfs.model.Chunk;
import com.dfs.util.ChecksumUtil;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Local on-disk chunk store.
 * Each chunk is stored as a file named {chunkId} under {dfs.storage.root}.
 * SHA-256 is verified on every read; mismatch throws ChunkCorruptedException.
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    @Value("${dfs.storage.root}")
    private String storageRoot;

    private Path rootPath;

    @PostConstruct
    public void init() throws IOException {
        rootPath = Paths.get(storageRoot).toAbsolutePath().normalize();
        Files.createDirectories(rootPath);
        log.info("ChunkStore initialized at {}", rootPath);
    }

    /** Writes the bytes as a new chunk and returns its metadata. */
    public Chunk write(byte[] data) throws IOException {
        String chunkId = UUID.randomUUID().toString();
        String sha = ChecksumUtil.sha256Hex(data);
        Path target = rootPath.resolve(chunkId);
        Files.write(target, data);
        log.debug("Wrote chunk {} ({} bytes, sha256={})", chunkId, data.length, sha);
        return new Chunk(chunkId, data.length, sha);
    }

    /** Reads a chunk by id and verifies its checksum against the expected value. */
    public byte[] read(Chunk chunk) throws IOException {
        Path target = rootPath.resolve(chunk.chunkId());
        if (!Files.exists(target)) {
            throw new IOException("Chunk not found: " + chunk.chunkId());
        }
        byte[] data = Files.readAllBytes(target);
        String actual = ChecksumUtil.sha256Hex(data);
        if (!actual.equals(chunk.sha256Hex())) {
            throw new ChunkCorruptedException(
                    "Checksum mismatch for chunk " + chunk.chunkId() +
                            " expected=" + chunk.sha256Hex() + " actual=" + actual);
        }
        return data;
    }

    public boolean delete(String chunkId) throws IOException {
        return Files.deleteIfExists(rootPath.resolve(chunkId));
    }

    public Path getRootPath() {
        return rootPath;
    }
}