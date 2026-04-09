package com.dfs.erasure;

import com.backblaze.erasure.ReedSolomon;
import com.dfs.storage.ShardStore;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Wraps Backblaze's ReedSolomon to encode each chunk into N data + M parity shards
 * and decode any (data + parity) >= N back into the original bytes.
 *
 * Layout per encoded chunk on disk:
 *   {chunkId}/0       <- data shard 0
 *   {chunkId}/1       <- data shard 1
 *   ...
 *   {chunkId}/{N-1}   <- data shard N-1
 *   {chunkId}/{N}     <- parity shard 0
 *   ...
 *   {chunkId}/{N+M-1} <- parity shard M-1
 *
 * The original chunk length is prepended (4 bytes, big-endian) to data shard 0
 * so we can strip padding on decode.
 */
@Service
public class ErasureCodingService {

    private static final Logger log = LoggerFactory.getLogger(ErasureCodingService.class);
    private static final int LENGTH_HEADER_BYTES = 4;

    private final ShardStore shardStore;
    private final int dataShards;
    private final int parityShards;
    private final int totalShards;

    private ReedSolomon codec;

    public ErasureCodingService(ShardStore shardStore,
                                @Value("${dfs.ec.data-shards}") int dataShards,
                                @Value("${dfs.ec.parity-shards}") int parityShards) {
        this.shardStore = shardStore;
        this.dataShards = dataShards;
        this.parityShards = parityShards;
        this.totalShards = dataShards + parityShards;
    }

    @PostConstruct
    public void init() {
        codec = ReedSolomon.create(dataShards, parityShards);
        log.info("ErasureCodingService initialized with {} data + {} parity shards", dataShards, parityShards);
    }

    /**
     * Encodes the chunk bytes into shards and writes them via ShardStore.
     * Returns the total stored bytes (useful for stats).
     */
    public long encodeAndStore(String chunkId, byte[] chunkData) throws IOException {
        // Each shard must be the same size. We pack [4-byte length][chunkData][zero pad]
        // into one buffer and split it into dataShards equal-sized pieces.
        int storedSize = LENGTH_HEADER_BYTES + chunkData.length;
        int shardSize = (storedSize + dataShards - 1) / dataShards; // ceil
        int bufferSize = shardSize * dataShards;

        byte[] packed = new byte[bufferSize];
        ByteBuffer.wrap(packed).putInt(chunkData.length);
        System.arraycopy(chunkData, 0, packed, LENGTH_HEADER_BYTES, chunkData.length);
        // Remaining bytes are already zero (Java default)

        byte[][] shards = new byte[totalShards][shardSize];
        for (int i = 0; i < dataShards; i++) {
            System.arraycopy(packed, i * shardSize, shards[i], 0, shardSize);
        }
        // Generate parity shards in-place
        codec.encodeParity(shards, 0, shardSize);

        long total = 0;
        for (int i = 0; i < totalShards; i++) {
            shardStore.writeShard(chunkId, i, shards[i]);
            total += shards[i].length;
        }
        log.debug("Encoded chunk {}: {} bytes -> {} shards of {} bytes each",
                chunkId, chunkData.length, totalShards, shardSize);
        return total;
    }

    /**
     * Reads shards and decodes the original chunk. Tolerates up to {parityShards} missing shards.
     * Throws IOException if too many shards are missing to recover.
     */
    public byte[] readAndDecode(String chunkId) throws IOException {
        byte[][] shards = new byte[totalShards][];
        boolean[] present = new boolean[totalShards];
        int presentCount = 0;
        int shardSize = -1;

        for (int i = 0; i < totalShards; i++) {
            byte[] data = shardStore.readShard(chunkId, i);
            if (data != null) {
                shards[i] = data;
                present[i] = true;
                presentCount++;
                if (shardSize < 0) shardSize = data.length;
            }
        }

        if (presentCount < dataShards) {
            throw new IOException("Cannot recover chunk " + chunkId + ": only " +
                    presentCount + "/" + totalShards + " shards available, need " + dataShards);
        }

        // Allocate empty buffers for missing shards (Reed-Solomon will fill them in)
        for (int i = 0; i < totalShards; i++) {
            if (!present[i]) {
                shards[i] = new byte[shardSize];
            }
        }

        // Reconstruct any missing shards
        if (presentCount < totalShards) {
            codec.decodeMissing(shards, present, 0, shardSize);
            log.debug("Recovered chunk {} from {} shards (reconstructed {})",
                    chunkId, presentCount, totalShards - presentCount);
        }

        // Reassemble the packed buffer
        byte[] packed = new byte[shardSize * dataShards];
        for (int i = 0; i < dataShards; i++) {
            System.arraycopy(shards[i], 0, packed, i * shardSize, shardSize);
        }

        // Strip the length header + padding
        int originalLength = ByteBuffer.wrap(packed).getInt();
        if (originalLength < 0 || originalLength > packed.length - LENGTH_HEADER_BYTES) {
            throw new IOException("Corrupt length header for chunk " + chunkId + ": " + originalLength);
        }
        byte[] result = new byte[originalLength];
        System.arraycopy(packed, LENGTH_HEADER_BYTES, result, 0, originalLength);
        return result;
    }

    public int getDataShards() { return dataShards; }
    public int getParityShards() { return parityShards; }
    public int getTotalShards() { return totalShards; }
}