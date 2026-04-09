package com.dfs.controller;

import com.dfs.model.FileMetadata;
import com.dfs.service.FileService;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/files")
public class FileController {

    private final FileService fileService;

    public FileController(FileService fileService) {
        this.fileService = fileService;
    }

    /** POST /api/files  (multipart form, field name = "file") */
    @PostMapping
    public ResponseEntity<FileMetadata> upload(@RequestParam("file") MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        FileMetadata meta = fileService.upload(file.getOriginalFilename(), file.getInputStream());
        return ResponseEntity.ok(meta);
    }

    /** GET /api/files */
    @GetMapping
    public List<FileMetadata> list() {
        return fileService.listAll();
    }

    /** GET /api/files/{fileId}/info */
    @GetMapping("/{fileId}/info")
    public ResponseEntity<FileMetadata> info(@PathVariable String fileId) {
        Optional<FileMetadata> meta = fileService.getMetadata(fileId);
        return meta.map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    /** GET /api/files/{fileId}  -> raw bytes download */
    @GetMapping("/{fileId}")
    public ResponseEntity<InputStreamResource> download(@PathVariable String fileId) throws IOException {
        Optional<FileMetadata> metaOpt = fileService.getMetadata(fileId);
        if (metaOpt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        FileMetadata meta = metaOpt.get();

        // Buffer in memory for now — Day 6 will switch to true streaming when we have
        // remote storage nodes. For demo files (a few MB) this is fine.
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) meta.sizeBytes());
        fileService.download(fileId, baos);
        byte[] bytes = baos.toByteArray();

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + meta.name() + "\"")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .contentLength(bytes.length)
                .body(new InputStreamResource(new ByteArrayInputStream(bytes)));
    }

    /** DELETE /api/files/{fileId} */
    @DeleteMapping("/{fileId}")
    public ResponseEntity<Void> delete(@PathVariable String fileId) throws IOException {
        boolean removed = fileService.delete(fileId);
        return removed ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }
}