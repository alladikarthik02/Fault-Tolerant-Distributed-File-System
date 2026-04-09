package com.dfs;

import com.dfs.model.FileMetadata;
import com.dfs.service.FileService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "dfs.storage.root=./build/test-data/chunks-concurrent",
        "dfs.metadata.journal-path=./build/test-data/meta-concurrent/journal.log",
        "dfs.shard.root=./build/test-data/shards-concurrent",
        "dfs.chunk.size-bytes=4096",
        "dfs.lease.ttl-seconds=30"
})
class ConcurrencyTest {

    @Autowired
    private FileService fileService;

    @Test
    void twelveConcurrentUploadsAllSucceed() throws Exception {
        int clients = 12;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch ready = new CountDownLatch(clients);

        // Pre-generate the payloads so all threads have something distinct
        byte[][] payloads = new byte[clients][];
        for (int i = 0; i < clients; i++) {
            payloads[i] = new byte[20_000 + i * 1000]; // varying sizes
            new Random(i).nextBytes(payloads[i]);
        }

        List<Future<FileMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                ready.countDown();
                startGate.await(); // fire all at the same instant
                return fileService.upload("client-" + idx + ".bin",
                        new ByteArrayInputStream(payloads[idx]));
            }));
        }

        ready.await(); // wait until every thread is parked at the gate
        startGate.countDown(); // GO

        // Collect results
        List<FileMetadata> uploaded = new ArrayList<>();
        for (Future<FileMetadata> f : futures) {
            uploaded.add(f.get(60, TimeUnit.SECONDS));
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        // Verify each one downloads correctly
        assertEquals(clients, uploaded.size());
        for (int i = 0; i < clients; i++) {
            FileMetadata meta = uploaded.get(i);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            fileService.download(meta.fileId(), out);
            assertArrayEquals(payloads[i], out.toByteArray(),
                    "Download mismatch for client " + i);
        }
    }
}