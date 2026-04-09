package com.dfs.model;

import java.time.Instant;
import java.util.List;

/**
 * Metadata for a logical file stored in the DFS.
 * The file's bytes are reconstructed by concatenating its chunks in order.
 *
 * @param fileId    unique id (UUID)
 * @param name      original filename supplied by client
 * @param sizeBytes total file size in bytes
 * @param chunks    ordered list of chunk metadata
 * @param createdAt server-side creation timestamp (used for TTL tier migration on Day 7)
 */
public record FileMetadata(
        String fileId,
        String name,
        long sizeBytes,
        List<Chunk> chunks,
        Instant createdAt
) {
}