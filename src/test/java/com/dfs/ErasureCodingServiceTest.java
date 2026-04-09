package com.dfs;

import com.dfs.erasure.ErasureCodingService;
import com.dfs.storage.ShardStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
        "dfs.storage.root=./build/test-data/chunks-ec",
        "dfs.metadata.journal-path=./build/test-data/meta-ec/journal.log",
        "dfs.shard.root=./build/test-data/shards-ec",
        "dfs.ec.data-shards=6",
        "dfs.ec.parity-shards=3"
})
class ErasureCodingServiceTest {

    @Autowired
    private ErasureCodingService ec;

    @Autowired
    private ShardStore shardStore;

    @Test
    void encodeDecodeRoundTrip() throws Exception {
        byte[] data = new byte[10_000];
        new Random(1).nextBytes(data);

        ec.encodeAndStore("chunk-rt", data);
        byte[] back = ec.readAndDecode("chunk-rt");

        assertArrayEquals(data, back);
    }

    @Test
    void recoversFromOneMissingShard() throws Exception {
        byte[] data = new byte[10_000];
        new Random(2).nextBytes(data);

        ec.encodeAndStore("chunk-1miss", data);
        // Delete one data shard
        shardStore.deleteShard("chunk-1miss", 2);

        byte[] back = ec.readAndDecode("chunk-1miss");
        assertArrayEquals(data, back);
    }

    @Test
    void recoversFromThreeMissingShards() throws Exception {
        byte[] data = new byte[10_000];
        new Random(3).nextBytes(data);

        ec.encodeAndStore("chunk-3miss", data);
        // Delete the maximum we can lose: 3 shards (mix of data + parity)
        shardStore.deleteShard("chunk-3miss", 0);
        shardStore.deleteShard("chunk-3miss", 4);
        shardStore.deleteShard("chunk-3miss", 7);

        byte[] back = ec.readAndDecode("chunk-3miss");
        assertArrayEquals(data, back);
    }

    @Test
    void failsWhenTooManyShardsMissing() throws Exception {
        byte[] data = new byte[10_000];
        new Random(4).nextBytes(data);

        ec.encodeAndStore("chunk-toomany", data);
        // Delete 4 shards — one more than we can tolerate
        shardStore.deleteShard("chunk-toomany", 0);
        shardStore.deleteShard("chunk-toomany", 1);
        shardStore.deleteShard("chunk-toomany", 2);
        shardStore.deleteShard("chunk-toomany", 3);

        assertThrows(java.io.IOException.class, () -> ec.readAndDecode("chunk-toomany"));
    }

    @Test
    void smallPayload() throws Exception {
        byte[] data = "tiny".getBytes();
        ec.encodeAndStore("chunk-tiny", data);
        assertArrayEquals(data, ec.readAndDecode("chunk-tiny"));
    }
}