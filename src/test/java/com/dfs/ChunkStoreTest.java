package com.dfs;

import com.dfs.model.Chunk;
import com.dfs.storage.ChunkCorruptedException;
import com.dfs.storage.ChunkStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "dfs.storage.root=./build/test-data/chunks"
})
class ChunkStoreTest {

    @Autowired
    private ChunkStore store;

    @Test
    void writeThenReadRoundTrip() throws Exception {
        byte[] payload = "hello distributed world".getBytes();
        Chunk c = store.write(payload);

        assertEquals(payload.length, c.sizeBytes());
        assertNotNull(c.sha256Hex());

        byte[] readBack = store.read(c);
        assertArrayEquals(payload, readBack);
    }

    @Test
    void corruptionIsDetected() throws Exception {
        byte[] payload = "important data".getBytes();
        Chunk c = store.write(payload);

        // Tamper with the file on disk
        Path file = store.getRootPath().resolve(c.chunkId());
        byte[] tampered = Arrays.copyOf(payload, payload.length);
        tampered[0] ^= 0x01;
        Files.write(file, tampered);

        assertThrows(ChunkCorruptedException.class, () -> store.read(c));
    }
}