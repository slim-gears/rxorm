package com.slimgears.rxrepo.util;

import com.slimgears.util.generic.ScopedInstance;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.slimgears.util.generic.MoreStrings.lazy;

public class CachedRoundRobinSchedulingProvider implements SchedulingProvider {
    private final static Logger log = LoggerFactory.getLogger(CachedRoundRobinSchedulingProvider.class);
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
        return create(maxExecutors, maxIdleTime, Function.identity());
    }

    public static SchedulingProvider create(int maxExecutors, Duration maxIdleTime, Function<Executor, Executor> executorDecorator) {
        return new CachedRoundRobinSchedulingProvider(maxExecutors, () ->
                executorDecorator.apply(new ThreadPoolExecutor(
                        0, 1,
                        maxIdleTime.toMillis(), TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>())));
    }

    protected Executor createExecutor() {
        return executorSupplier.get();
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
    public <T> T scope(Callable<T> runnable) {
        Callable<T> loggingRunnable = () -> {
            Object lazyIndex = lazy(() -> executors.indexOf(currentExecutor.current()));
            try {
                log.trace(">>> Entering scope (Queue #{})", lazyIndex);
                return runnable.call();
            } finally {
                log.trace("<<< Left scope (Queue #{})", lazyIndex);
            }
        };
        if (currentExecutor.current() != null) {
            try {
                return loggingRunnable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            return currentExecutor.withScope(getExecutor(), loggingRunnable);
        }
    }

    @Override
    public Scheduler scheduler() {
        Executor executor = currentExecutor.current();
        if (executor != null) {
            log.trace("Scoped executor found: Queue #{}", lazy(() -> executors.indexOf(executor)));
            return Schedulers.from(executor);
        }
        log.trace("Scoped executor not found.");
        return Schedulers.from(Runnable::run);
    }
}
