package com.slimgears.rxrepo.util;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedRoundRobinExecutorPool implements ExecutorPool {
    private final int maxExecutors;
    private final Duration maxIdleTime;
    private final List<Executor> executors = new ArrayList<>();
    private final AtomicInteger nextQueueIndex = new AtomicInteger();

    private CachedRoundRobinExecutorPool(int maxExecutors, Duration maxIdleTime) {
        this.maxExecutors = maxExecutors;
        this.maxIdleTime = maxIdleTime;
    }

    public static ExecutorPool create(int maxExecutors, Duration maxIdleTime) {
        return new CachedRoundRobinExecutorPool(maxExecutors, maxIdleTime);
    }

    protected Executor createExecutor() {
        return new ThreadPoolExecutor(0, 1,
                maxIdleTime.toMillis(), TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
    }

    @Override
    public Executor getExecutor() {
        if (executors.size() < maxExecutors) {
            Executor executor = createExecutor();
            executors.add(executor);
            return executor;
        }
        return executors.get(nextQueueIndex.getAndUpdate(index -> (index + 1) % executors.size()));
    }
}
