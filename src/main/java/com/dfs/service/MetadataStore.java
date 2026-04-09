package com.dfs.service;

import com.dfs.model.FileMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory map of fileId -> FileMetadata, backed by an append-only JSON-lines journal.
 *
 * Journal format: one record per line:
 *   {"op":"PUT","meta":{...}}
 *   {"op":"DELETE","fileId":"..."}
 *
 * On startup, the journal is replayed to rebuild in-memory state. This is the same
 * recovery model HDFS uses for its NameNode edit log.
 */
@Service
public class MetadataStore {

    private static final Logger log = LoggerFactory.getLogger(MetadataStore.class);

    @Value("${dfs.metadata.journal-path:./data/metadata/journal.log}")
    private String journalPathStr;

    private Path journalPath;
    private final Map<String, FileMetadata> store = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @PostConstruct
    public void init() throws IOException {
        journalPath = Paths.get(journalPathStr).toAbsolutePath().normalize();
        Files.createDirectories(journalPath.getParent());
        if (!Files.exists(journalPath)) {
            Files.createFile(journalPath);
        }
        replay();
        log.info("MetadataStore loaded {} files from {}", store.size(), journalPath);
    }

    private void replay() throws IOException {
        List<String> lines = Files.readAllLines(journalPath, StandardCharsets.UTF_8);
        int applied = 0;
        for (String line : lines) {
            if (line.isBlank()) continue;
            try {
                JournalRecord rec = mapper.readValue(line, JournalRecord.class);
                if ("PUT".equals(rec.op) && rec.meta != null) {
                    store.put(rec.meta.fileId(), rec.meta);
                    applied++;
                } else if ("DELETE".equals(rec.op) && rec.fileId != null) {
                    store.remove(rec.fileId);
                    applied++;
                }
            } catch (Exception e) {
                log.warn("Skipping malformed journal line: {}", line, e);
            }
        }
        log.info("Replayed {} journal records", applied);
    }

    /** Append-then-update: durability before visibility. */
    public synchronized void put(FileMetadata meta) throws IOException {
        JournalRecord rec = new JournalRecord("PUT", meta, null);
        appendJournal(rec);
        store.put(meta.fileId(), meta);
    }

    public synchronized void delete(String fileId) throws IOException {
        JournalRecord rec = new JournalRecord("DELETE", null, fileId);
        appendJournal(rec);
        store.remove(fileId);
    }

    public Optional<FileMetadata> get(String fileId) {
        return Optional.ofNullable(store.get(fileId));
    }

    public List<FileMetadata> list() {
        return List.copyOf(store.values());
    }

    public int size() {
        return store.size();
    }

    private void appendJournal(JournalRecord rec) throws IOException {
        String json = mapper.writeValueAsString(rec);
        try (BufferedWriter w = Files.newBufferedWriter(
                journalPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(json);
            w.newLine();
        }
    }

    /** Public for tests that want to wipe state between runs. */
    public Path getJournalPath() {
        return journalPath;
    }

    /** Journal record DTO. Public so Jackson can deserialize. */
    public static class JournalRecord {
        public String op;
        public FileMetadata meta;
        public String fileId;

        public JournalRecord() {}

        public JournalRecord(String op, FileMetadata meta, String fileId) {
            this.op = op;
            this.meta = meta;
            this.fileId = fileId;
        }
    }
}