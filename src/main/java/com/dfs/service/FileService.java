package com.dfs.service;

import com.dfs.model.FileMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

/**
 * High-level facade. Coordinates ChunkingService (which handles chunking + EC)
 * and MetadataStore (which handles the journal). Controllers talk only to this.
 */
@Service
public class FileService {

    private static final Logger log = LoggerFactory.getLogger(FileService.class);

    private final ChunkingService chunking;
    private final MetadataStore metadata;

    public FileService(ChunkingService chunking, MetadataStore metadata) {
        this.chunking = chunking;
        this.metadata = metadata;
    }

    public FileMetadata upload(String name, InputStream in) throws IOException {
        FileMetadata meta = chunking.storeFile(name, in);
        metadata.put(meta);
        log.info("Uploaded file {} ({} bytes, {} chunks)", meta.fileId(), meta.sizeBytes(), meta.chunks().size());
        return meta;
    }

    public Optional<FileMetadata> getMetadata(String fileId) {
        return metadata.get(fileId);
    }

    public void download(String fileId, OutputStream out) throws IOException {
        FileMetadata meta = metadata.get(fileId)
                .orElseThrow(() -> new IOException("File not found: " + fileId));
        chunking.readFile(meta, out);
    }

    public List<FileMetadata> listAll() {
        return metadata.list();
    }

    public boolean delete(String fileId) throws IOException {
        if (metadata.get(fileId).isEmpty()) {
            return false;
        }
        metadata.delete(fileId);
        // Note: shard cleanup will be added on Day 7 alongside tier migration.
        // For now, deleted files leak shards on disk — fine for Day 4.
        log.info("Deleted file {}", fileId);
        return true;
    }
}