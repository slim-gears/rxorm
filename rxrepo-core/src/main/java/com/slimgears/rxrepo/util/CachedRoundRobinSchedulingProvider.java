package com.slimgears.rxrepo.util;

import com.slimgears.nanometer.MetricCollector;
import com.slimgears.nanometer.MetricTag;
import com.slimgears.nanometer.Metrics;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedRoundRobinSchedulingProvider implements SchedulingProvider {
    private final static Logger log = LoggerFactory.getLogger(CachedRoundRobinSchedulingProvider.class);
    private final static MetricCollector metrics = Metrics.collector(CachedRoundRobinSchedulingProvider.class);
    private final int maxExecutors;
    private final Duration maxIdleTime;
    private final List<Executor> executors = new ArrayList<>();
    private final AtomicInteger nextQueueIndex = new AtomicInteger();
    private final Map<Executor, AtomicInteger> executorToQueueSizeMap = new HashMap<>();
    private final Map<Executor, Integer> executorToIndexMap = new HashMap<>();

    private CachedRoundRobinSchedulingProvider(int maxExecutors, Duration maxIdleTime) {
        this.maxExecutors = maxExecutors;
        this.maxIdleTime = maxIdleTime;
    }

    public static SchedulingProvider create(int maxExecutors, Duration maxIdleTime) {
        return new CachedRoundRobinSchedulingProvider(maxExecutors, maxIdleTime);
    }

    protected Executor createExecutor() {
        Executor executor = new ThreadPoolExecutor(0, 1,
                maxIdleTime.toMillis(), TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        executorToQueueSizeMap.put(executor, new AtomicInteger());
        int queueIndex = executorToIndexMap.size();
        log.debug("Adding queue #{}", queueIndex);
        executorToIndexMap.put(executor, queueIndex);
        return executor;
    }

    protected Executor getExecutor() {
        if (executors.size() < maxExecutors) {
            Executor executor = createExecutor();
            executors.add(executor);
            return executor;
        }
        return executors.get(nextQueueIndex.getAndUpdate(index -> (index + 1) % executors.size()));
    }

    @Override
    public <T> Observable<T> applyScheduler(Observable<T> observable) {
        Executor executor = getExecutor();
        AtomicInteger queueSize = executorToQueueSizeMap.get(executor);
        int queueIndex = executorToIndexMap.get(executor);
        MetricCollector.Gauge gauge = metrics.gauge("queueSize", MetricTag.of("#" + queueIndex));
        return observable
                .doOnNext(val -> {
                    int newSize = queueSize.incrementAndGet();
                    gauge.record(newSize);
                    log.debug("Added task to queue #{}, new size: {}", queueIndex, newSize);
                })
                .observeOn(Schedulers.from(executor))
                .doOnNext(val -> {
                    int newSize = queueSize.decrementAndGet();
                    gauge.record(newSize);
                    log.debug("Task from to queue #{} completed, new size: {}", queueIndex, newSize);
                });
    }
}
