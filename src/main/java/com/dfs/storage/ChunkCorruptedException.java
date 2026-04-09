package com.dfs.storage;

import java.io.IOException;

public class ChunkCorruptedException extends IOException {
    public ChunkCorruptedException(String message) {
        super(message);
    }
}