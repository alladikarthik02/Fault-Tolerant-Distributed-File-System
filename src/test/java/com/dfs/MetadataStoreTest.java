package com.dfs;

import com.dfs.model.Chunk;
import com.dfs.model.FileMetadata;
import com.dfs.service.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.nio.file.Files;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "dfs.storage.root=./build/test-data/chunks-meta",
        "dfs.metadata.journal-path=./build/test-data/meta-store/journal.log"
})
class MetadataStoreTest {

    @Autowired
    private MetadataStore store;

    @BeforeEach
    void clean() throws Exception {
        // Wipe journal so each test starts fresh — but only the file, the bean stays
        Files.deleteIfExists(store.getJournalPath());
        Files.createFile(store.getJournalPath());
        // Clear in-memory state by deleting every known file
        for (FileMetadata m : store.list()) {
            store.delete(m.fileId());
        }
        Files.deleteIfExists(store.getJournalPath());
        Files.createFile(store.getJournalPath());
    }

    @Test
    void putAndGet() throws Exception {
        FileMetadata m = sample("file-1", "a.bin");
        store.put(m);

        assertTrue(store.get("file-1").isPresent());
        assertEquals("a.bin", store.get("file-1").get().name());
    }

    @Test
    void journalSurvivesRebuild() throws Exception {
        store.put(sample("file-A", "a.bin"));
        store.put(sample("file-B", "b.bin"));

        // Build a fresh MetadataStore pointing at the same journal and replay
        MetadataStore reloaded = new MetadataStore();
        // mimic Spring injection
        var f = MetadataStore.class.getDeclaredField("journalPathStr");
        f.setAccessible(true);
        f.set(reloaded, store.getJournalPath().toString());
        reloaded.init();

        assertEquals(2, reloaded.size());
        assertTrue(reloaded.get("file-A").isPresent());
        assertTrue(reloaded.get("file-B").isPresent());
    }

    @Test
    void deleteIsJournaled() throws Exception {
        store.put(sample("file-X", "x.bin"));
        store.delete("file-X");

        MetadataStore reloaded = new MetadataStore();
        var f = MetadataStore.class.getDeclaredField("journalPathStr");
        f.setAccessible(true);
        f.set(reloaded, store.getJournalPath().toString());
        reloaded.init();

        assertTrue(reloaded.get("file-X").isEmpty());
    }

    private static FileMetadata sample(String id, String name) {
        Chunk c = new Chunk("chunk-" + id, 10, "deadbeef");
        return new FileMetadata(id, name, 10, List.of(c), Instant.now());
    }
}