package com.slimgears.rxrepo.util;

import java.util.concurrent.Semaphore;

public class SemaphoreLockProvider implements LockProvider {
    private final Semaphore semaphore = new Semaphore(1);

    private SemaphoreLockProvider() {

    }

    public static LockProvider create() {
        return new SemaphoreLockProvider();
    }

    @Override
    public AutoCloseable lock() {
        try {
            semaphore.acquire();
            return semaphore::release;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
