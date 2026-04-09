package com.dfs.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Issues time-limited write leases on resource ids (typically fileIds).
 * Only one lease can be held per resource at a time. Expired leases are
 * reclaimed lazily on the next acquire/check.
 *
 * This is the same model HDFS uses for write leases on its NameNode:
 * a client must hold a valid lease to write, and the lease must be renewed
 * periodically or the write is aborted.
 */
@Service
public class LeaseManager {

    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);

    private final long ttlSeconds;
    private final Map<String, Lease> leases = new ConcurrentHashMap<>();

    public LeaseManager(@Value("${dfs.lease.ttl-seconds}") long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    /**
     * Try to acquire a lease on the given resource. Returns the lease token
     * (a UUID) if successful, or empty if another client already holds a
     * non-expired lease on this resource.
     */
    public synchronized Optional<String> acquire(String resourceId) {
        Lease existing = leases.get(resourceId);
        if (existing != null && !existing.isExpired()) {
            log.debug("Lease acquire denied for {}: held by {} until {}",
                    resourceId, existing.token, existing.expiresAt);
            return Optional.empty();
        }
        if (existing != null && existing.isExpired()) {
            log.info("Reclaiming expired lease on {} (was held by {})", resourceId, existing.token);
        }
        String token = UUID.randomUUID().toString();
        Lease lease = new Lease(token, Instant.now().plusSeconds(ttlSeconds));
        leases.put(resourceId, lease);
        log.debug("Lease granted on {} -> {} (expires {})", resourceId, token, lease.expiresAt);
        return Optional.of(token);
    }

    /** Renew an existing lease. Throws if the token doesn't match or the lease has expired. */
    public synchronized void renew(String resourceId, String token) {
        Lease lease = leases.get(resourceId);
        if (lease == null || !lease.token.equals(token)) {
            throw new LeaseExpiredException("No matching lease for " + resourceId);
        }
        if (lease.isExpired()) {
            leases.remove(resourceId);
            throw new LeaseExpiredException("Lease expired for " + resourceId);
        }
        lease.expiresAt = Instant.now().plusSeconds(ttlSeconds);
        log.debug("Lease renewed on {} -> {} (new expiry {})", resourceId, token, lease.expiresAt);
    }

    /** Release a lease the caller no longer needs. No-op if the token doesn't match. */
    public synchronized void release(String resourceId, String token) {
        Lease lease = leases.get(resourceId);
        if (lease != null && lease.token.equals(token)) {
            leases.remove(resourceId);
            log.debug("Lease released on {}", resourceId);
        }
    }

    /** Validate that the given token is a live lease for the resource. */
    public synchronized boolean isHeldBy(String resourceId, String token) {
        Lease lease = leases.get(resourceId);
        if (lease == null || !lease.token.equals(token)) return false;
        if (lease.isExpired()) {
            leases.remove(resourceId);
            return false;
        }
        return true;
    }

    /** Useful for tests. */
    public int activeLeaseCount() {
        return (int) leases.values().stream().filter(l -> !l.isExpired()).count();
    }

    public Duration getTtl() {
        return Duration.ofSeconds(ttlSeconds);
    }

    private static class Lease {
        final String token;
        Instant expiresAt;

        Lease(String token, Instant expiresAt) {
            this.token = token;
            this.expiresAt = expiresAt;
        }

        boolean isExpired() {
            return Instant.now().isAfter(expiresAt);
        }
    }
}