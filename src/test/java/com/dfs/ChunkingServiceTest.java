package com.dfs;

import com.dfs.model.FileMetadata;
import com.dfs.service.ChunkingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "dfs.storage.root=./build/test-data/chunks-chunking",
        "dfs.metadata.journal-path=./build/test-data/meta-chunking/journal.log",
        "dfs.chunk.size-bytes=1024"
})
class ChunkingServiceTest {

    @Autowired
    private ChunkingService chunking;

    @Test
    void exactMultipleOfChunkSize() throws Exception {
        byte[] data = new byte[1024 * 4];
        new Random(42).nextBytes(data);

        FileMetadata meta = chunking.storeFile("exact.bin", new ByteArrayInputStream(data));
        assertEquals(4, meta.chunks().size());
        assertEquals(data.length, meta.sizeBytes());

        byte[] back = chunking.readFileBytes(meta);
        assertArrayEquals(data, back);
    }

    @Test
    void partialTailChunk() throws Exception {
        byte[] data = new byte[1024 * 3 + 17];
        new Random(7).nextBytes(data);

        FileMetadata meta = chunking.storeFile("tail.bin", new ByteArrayInputStream(data));
        assertEquals(4, meta.chunks().size());
        assertEquals(17, meta.chunks().get(3).sizeBytes());

        assertArrayEquals(data, chunking.readFileBytes(meta));
    }

    @Test
    void smallerThanOneChunk() throws Exception {
        byte[] data = "tiny payload".getBytes();
        FileMetadata meta = chunking.storeFile("tiny.txt", new ByteArrayInputStream(data));
        assertEquals(1, meta.chunks().size());
        assertArrayEquals(data, chunking.readFileBytes(meta));
    }
}