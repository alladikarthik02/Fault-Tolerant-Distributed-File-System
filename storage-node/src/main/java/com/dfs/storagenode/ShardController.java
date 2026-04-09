package com.dfs.storagenode;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@RestController
@RequestMapping("/shards")
public class ShardController {

    private static final Logger log = LoggerFactory.getLogger(ShardController.class);

    @Value("${storagenode.data-dir}")
    private String dataDir;

    @Value("${storagenode.id}")
    private String nodeId;

    private Path rootPath;

    @PostConstruct
    public void init() throws IOException {
        rootPath = Paths.get(dataDir).toAbsolutePath().normalize();
        Files.createDirectories(rootPath);
        log.info("Storage node {} starting at {}", nodeId, rootPath);
    }

    /** PUT /shards/{chunkId}/{shardIndex}  — body is the raw shard bytes. */
    @PutMapping(value = "/{chunkId}/{shardIndex}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<Map<String, Object>> put(@PathVariable String chunkId,
                                                    @PathVariable int shardIndex,
                                                    @RequestBody byte[] data) throws IOException {
        Path dir = rootPath.resolve(chunkId);
        Files.createDirectories(dir);
        Path file = dir.resolve(String.valueOf(shardIndex));
        Files.write(file, data);
        log.debug("Stored shard {}/{} ({} bytes)", chunkId, shardIndex, data.length);
        return ResponseEntity.ok(Map.of(
                "nodeId", nodeId,
                "chunkId", chunkId,
                "shardIndex", shardIndex,
                "bytes", data.length));
    }

    /** GET /shards/{chunkId}/{shardIndex}  — returns the raw shard bytes, or 404. */
    @GetMapping(value = "/{chunkId}/{shardIndex}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<byte[]> get(@PathVariable String chunkId,
                                      @PathVariable int shardIndex) throws IOException {
        Path file = rootPath.resolve(chunkId).resolve(String.valueOf(shardIndex));
        if (!Files.exists(file)) {
            return ResponseEntity.notFound().build();
        }
        byte[] data = Files.readAllBytes(file);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
                .contentLength(data.length)
                .body(data);
    }

    /** DELETE /shards/{chunkId}/{shardIndex} */
    @DeleteMapping("/{chunkId}/{shardIndex}")
    public ResponseEntity<Void> delete(@PathVariable String chunkId,
                                       @PathVariable int shardIndex) throws IOException {
        Path file = rootPath.resolve(chunkId).resolve(String.valueOf(shardIndex));
        boolean deleted = Files.deleteIfExists(file);
        return deleted ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    /**
     * GET /shards  — lists all (chunkId, shardIndex) pairs this node holds.
     * Used by the controller's rebuild service to figure out what's where.
     */
    @GetMapping
    public List<Map<String, Object>> list() throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        if (!Files.exists(rootPath)) return result;
        try (Stream<Path> chunks = Files.list(rootPath)) {
            for (Path chunkDir : (Iterable<Path>) chunks::iterator) {
                if (!Files.isDirectory(chunkDir)) continue;
                String chunkId = chunkDir.getFileName().toString();
                try (Stream<Path> shards = Files.list(chunkDir)) {
                    for (Path shardFile : (Iterable<Path>) shards::iterator) {
                        try {
                            int idx = Integer.parseInt(shardFile.getFileName().toString());
                            result.add(Map.of("chunkId", chunkId, "shardIndex", idx));
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }
        }
        return result;
    }
}