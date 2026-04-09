package com.dfs.model;

/**
 * Metadata for a single stored chunk.
 *
 * @param chunkId    unique id (UUID string)
 * @param sizeBytes  number of bytes in the chunk
 * @param sha256Hex  hex-encoded SHA-256 of the chunk bytes (used to detect corruption)
 */
public record Chunk(String chunkId, long sizeBytes, String sha256Hex) {
}