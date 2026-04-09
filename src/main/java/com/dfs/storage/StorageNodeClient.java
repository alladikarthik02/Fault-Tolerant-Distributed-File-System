package com.dfs.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

/**
 * HTTP client for a single storage node.
 * Wraps PUT/GET/DELETE on /shards/{chunkId}/{shardIndex} plus a /health probe.
 */
public class StorageNodeClient {

    private static final Logger log = LoggerFactory.getLogger(StorageNodeClient.class);

    private final String baseUrl;
    private final RestTemplate http;

    public StorageNodeClient(String baseUrl, RestTemplate http) {
        this.baseUrl = baseUrl;
        this.http = http;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    /** Returns true if the node responds 2xx on /health within the configured timeout. */
    public boolean isHealthy() {
        try {
            ResponseEntity<String> resp = http.getForEntity(baseUrl + "/health", String.class);
            return resp.getStatusCode().is2xxSuccessful();
        } catch (Exception e) {
            return false;
        }
    }

    public void putShard(String chunkId, int shardIndex, byte[] data) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        HttpEntity<byte[]> entity = new HttpEntity<>(data, headers);
        String url = baseUrl + "/shards/" + chunkId + "/" + shardIndex;
        http.exchange(url, HttpMethod.PUT, entity, String.class);
        log.debug("PUT {} ({} bytes)", url, data.length);
    }

    /** Returns null if the shard is not present (404), throws on transport errors. */
    public byte[] getShard(String chunkId, int shardIndex) {
        String url = baseUrl + "/shards/" + chunkId + "/" + shardIndex;
        try {
            ResponseEntity<byte[]> resp = http.getForEntity(url, byte[].class);
            return resp.getBody();
        } catch (HttpClientErrorException.NotFound e) {
            return null;
        }
    }

    /** Returns true if the node was reachable at all (so we can distinguish "missing" from "down"). */
    public boolean deleteShard(String chunkId, int shardIndex) {
        String url = baseUrl + "/shards/" + chunkId + "/" + shardIndex;
        try {
            http.delete(url);
            return true;
        } catch (HttpClientErrorException.NotFound e) {
            return true;
        } catch (ResourceAccessException e) {
            return false;
        }
    }
}