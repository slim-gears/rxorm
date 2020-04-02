package com.slimgears.rxrepo.util;

import com.slimgears.nanometer.MetricCollector;
import com.slimgears.nanometer.MetricTag;
import com.slimgears.nanometer.Metrics;
import com.slimgears.util.generic.ScopedInstance;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class CachedRoundRobinSchedulingProvider implements SchedulingProvider {
    private final static Logger log = LoggerFactory.getLogger(CachedRoundRobinSchedulingProvider.class);
    private final static MetricCollector metrics = Metrics.collector(CachedRoundRobinSchedulingProvider.class);
    private final int maxExecutors;
    private final Supplier<Executor> executorSupplier;
    private final List<Executor> executors = new ArrayList<>();
    private final AtomicInteger nextQueueIndex = new AtomicInteger();
    private final ScopedInstance<Executor> currentExecutor = ScopedInstance.create();

    private CachedRoundRobinSchedulingProvider(int maxExecutors, Supplier<Executor> executorSupplier) {
        this.maxExecutors = maxExecutors;
        this.executorSupplier = executorSupplier;
    }

    public static SchedulingProvider create(int maxExecutors, Supplier<Executor> executorSupplier) {
        return new CachedRoundRobinSchedulingProvider(maxExecutors, executorSupplier);
    }

    public static SchedulingProvider create(int maxExecutors, Duration maxIdleTime) {
        return new CachedRoundRobinSchedulingProvider(maxExecutors, () ->
                new ThreadPoolExecutor(
                        0, 1,
                        maxIdleTime.toMillis(), TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>()));
    }

    protected Executor createExecutor() {
        return executorSupplier.get();
    }

    protected Executor getExecutor() {
        if (executors.size() < maxExecutors) {
            Executor executor = withMetrics(createExecutor(), executors.size());
            executors.add(executor);
            return executor;
        }
        return executors.get(nextQueueIndex.getAndUpdate(index -> (index + 1) % executors.size()));
    }

    private Executor withMetrics(Executor executor, int queueIndex) {
        AtomicInteger queueSize = new AtomicInteger();
        MetricCollector.Gauge gauge = metrics.gauge("queueSize", MetricTag.of("#" + queueIndex));
        return runnable -> {
            int sizeBefore = queueSize.incrementAndGet();
            gauge.record(sizeBefore);
            log.debug("Added task to queue #{}, new size: {}", queueIndex, sizeBefore);
            executor.execute(() -> {
                int sizeAfter = queueSize.decrementAndGet();
                gauge.record(sizeAfter);
                log.debug("Task from to queue #{} completed, new size: {}", queueIndex, sizeAfter);
                runnable.run();
            });
        };
    }

    @Override
    public <T> T scope(Callable<T> runnable) {
        if (currentExecutor.current() != null) {
            try {
                return runnable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            return currentExecutor.withScope(getExecutor(), runnable);
        }
    }

    @Override
    public Scheduler scheduler() {
        Executor executor = Optional.ofNullable(currentExecutor.current()).orElseGet(this::getExecutor);
        return Schedulers.from(executor);
    }
}
