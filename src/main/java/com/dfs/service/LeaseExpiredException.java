package com.dfs.service;

public class LeaseExpiredException extends RuntimeException {
    public LeaseExpiredException(String message) {
        super(message);
    }
}