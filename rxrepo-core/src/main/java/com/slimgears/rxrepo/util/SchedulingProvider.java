package com.slimgears.rxrepo.util;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

import java.util.concurrent.Callable;

public interface SchedulingProvider {
    Scheduler scheduler();
    <T> T scope(Callable<T> callable);

    default void scope(Runnable runnable) {
        this.<Void>scope(() -> { runnable.run(); return null; });
    }

    static SchedulingProvider empty() {
        return new SchedulingProvider() {
            @Override
            public Scheduler scheduler() {
                return Schedulers.from(Runnable::run);
            }

            @Override
            public <T> T scope(Callable<T> callable) {
                try {
                    return callable.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
