package com.dfs.service;

import com.dfs.erasure.ErasureCodingService;
import com.dfs.model.Chunk;
import com.dfs.model.FileMetadata;
import com.dfs.util.ChecksumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Splits an arbitrary input stream into fixed-size chunks and routes each chunk
 * through the erasure coding pipeline. Reassembles them on read.
 */
@Service
public class ChunkingService {

    private static final Logger log = LoggerFactory.getLogger(ChunkingService.class);

    private final ErasureCodingService ec;
    private final int chunkSize;

    public ChunkingService(ErasureCodingService ec,
                           @Value("${dfs.chunk.size-bytes}") int chunkSize) {
        this.ec = ec;
        this.chunkSize = chunkSize;
    }

    /**
     * Splits the stream into chunks of {@link #chunkSize} bytes (last chunk may be smaller),
     * encodes each chunk via Reed-Solomon, and persists the resulting shards.
     */
    public FileMetadata storeFile(String name, InputStream in) throws IOException {
        String fileId = UUID.randomUUID().toString();
        List<Chunk> chunks = new ArrayList<>();
        long totalSize = 0;

        byte[] buffer = new byte[chunkSize];
        int filled = 0;
        int read;
        while ((read = in.read(buffer, filled, chunkSize - filled)) != -1) {
            filled += read;
            if (filled == chunkSize) {
                chunks.add(persistChunk(buffer));
                totalSize += filled;
                filled = 0;
                buffer = new byte[chunkSize];
            }
        }
        if (filled > 0) {
            byte[] tail = Arrays.copyOf(buffer, filled);
            chunks.add(persistChunk(tail));
            totalSize += filled;
        }

        FileMetadata meta = new FileMetadata(
                fileId, name, totalSize, List.copyOf(chunks), java.time.Instant.now());
        log.info("Stored file {} ({}) as {} chunks, {} bytes", name, fileId, chunks.size(), totalSize);
        return meta;
    }

    private Chunk persistChunk(byte[] data) throws IOException {
        String chunkId = UUID.randomUUID().toString();
        String sha = ChecksumUtil.sha256Hex(data);
        ec.encodeAndStore(chunkId, data);
        return new Chunk(chunkId, data.length, sha);
    }

    /**
     * Streams the chunks of a file back in order. Each chunk is decoded from its shards
     * (tolerating missing shards) and verified against its stored SHA-256.
     */
    public void readFile(FileMetadata meta, OutputStream out) throws IOException {
        for (Chunk c : meta.chunks()) {
            byte[] data = ec.readAndDecode(c.chunkId());
            String actualSha = ChecksumUtil.sha256Hex(data);
            if (!actualSha.equals(c.sha256Hex())) {
                throw new IOException("Checksum mismatch on chunk " + c.chunkId() +
                        " expected=" + c.sha256Hex() + " actual=" + actualSha);
            }
            out.write(data);
        }
        out.flush();
    }

    public byte[] readFileBytes(FileMetadata meta) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) meta.sizeBytes());
        readFile(meta, baos);
        return baos.toByteArray();
    }
}