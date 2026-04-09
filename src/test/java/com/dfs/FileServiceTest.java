package com.dfs;

import com.dfs.model.FileMetadata;
import com.dfs.service.FileService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "dfs.storage.root=./build/test-data/chunks-fs",
        "dfs.metadata.journal-path=./build/test-data/meta-fs/journal.log",
        "dfs.shard.root=./build/test-data/shards-fs",
        "dfs.chunk.size-bytes=2048",
        "dfs.ec.data-shards=6",
        "dfs.ec.parity-shards=3"
})
class FileServiceTest {

    @Autowired
    private FileService fileService;

    @Test
    void uploadDownloadRoundTrip() throws Exception {
        byte[] payload = new byte[10_000];
        new Random(99).nextBytes(payload);

        FileMetadata meta = fileService.upload("test.bin", new ByteArrayInputStream(payload));
        assertNotNull(meta.fileId());
        assertEquals(payload.length, meta.sizeBytes());
        assertTrue(meta.chunks().size() >= 4); // 10000 / 2048 = 5 chunks

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        fileService.download(meta.fileId(), out);
        assertArrayEquals(payload, out.toByteArray());
    }

    @Test
    void listIncludesUploadedFile() throws Exception {
        int before = fileService.listAll().size();
        fileService.upload("listed.bin", new ByteArrayInputStream("hi".getBytes()));
        assertEquals(before + 1, fileService.listAll().size());
    }

    @Test
    void deleteRemovesFromList() throws Exception {
        FileMetadata meta = fileService.upload("doomed.bin",
                new ByteArrayInputStream("bye".getBytes()));
        assertTrue(fileService.delete(meta.fileId()));
        assertTrue(fileService.getMetadata(meta.fileId()).isEmpty());
    }
}