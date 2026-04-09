package com.dfs;

import com.dfs.service.LeaseExpiredException;
import com.dfs.service.LeaseManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "dfs.storage.root=./build/test-data/chunks-lease",
        "dfs.metadata.journal-path=./build/test-data/meta-lease/journal.log",
        "dfs.shard.root=./build/test-data/shards-lease",
        "dfs.lease.ttl-seconds=30"
})
class LeaseManagerTest {

    @Autowired
    private LeaseManager leases;

    @Test
    void firstAcquireSucceeds() {
        Optional<String> token = leases.acquire("file-1");
        assertTrue(token.isPresent());
    }

    @Test
    void secondAcquireOnSameResourceFails() {
        leases.acquire("file-2");
        assertTrue(leases.acquire("file-2").isEmpty());
    }

    @Test
    void releaseAllowsReacquire() {
        String token = leases.acquire("file-3").orElseThrow();
        leases.release("file-3", token);
        assertTrue(leases.acquire("file-3").isPresent());
    }

    @Test
    void renewWithCorrectTokenSucceeds() {
        String token = leases.acquire("file-4").orElseThrow();
        assertDoesNotThrow(() -> leases.renew("file-4", token));
    }

    @Test
    void renewWithWrongTokenFails() {
        leases.acquire("file-5");
        assertThrows(LeaseExpiredException.class, () -> leases.renew("file-5", "wrong-token"));
    }

    @Test
    void isHeldByReturnsTrueForLiveLease() {
        String token = leases.acquire("file-6").orElseThrow();
        assertTrue(leases.isHeldBy("file-6", token));
        assertFalse(leases.isHeldBy("file-6", "other-token"));
    }
}