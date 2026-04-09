package com.dfs.controller;

import com.dfs.service.LeaseExpiredException;
import com.dfs.service.LeaseManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/leases")
public class LeaseController {

    private final LeaseManager leaseManager;

    public LeaseController(LeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    /** POST /api/leases/{resourceId}  -> { "token": "..." } or 409 if held */
    @PostMapping("/{resourceId}")
    public ResponseEntity<Map<String, String>> acquire(@PathVariable String resourceId) {
        Optional<String> token = leaseManager.acquire(resourceId);
        return token.map(t -> ResponseEntity.ok(Map.of("token", t, "resourceId", resourceId)))
                .orElseGet(() -> ResponseEntity.status(409)
                        .body(Map.of("error", "lease_held", "resourceId", resourceId)));
    }

    /** PUT /api/leases/{resourceId}/renew?token=... */
    @PutMapping("/{resourceId}/renew")
    public ResponseEntity<Map<String, String>> renew(@PathVariable String resourceId,
                                                     @RequestParam String token) {
        try {
            leaseManager.renew(resourceId, token);
            return ResponseEntity.ok(Map.of("status", "renewed", "resourceId", resourceId));
        } catch (LeaseExpiredException e) {
            return ResponseEntity.status(410).body(Map.of("error", e.getMessage()));
        }
    }

    /** DELETE /api/leases/{resourceId}?token=... */
    @DeleteMapping("/{resourceId}")
    public ResponseEntity<Void> release(@PathVariable String resourceId,
                                        @RequestParam String token) {
        leaseManager.release(resourceId, token);
        return ResponseEntity.noContent().build();
    }
}