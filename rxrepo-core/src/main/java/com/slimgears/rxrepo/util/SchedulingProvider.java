package com.slimgears.rxrepo.util;

import io.reactivex.Scheduler;

import java.util.concurrent.Callable;

public interface SchedulingProvider {
    Scheduler scheduler();
    <T> T scope(Callable<T> callable);

    default void scope(Runnable runnable) {
        this.<Void>scope(() -> { runnable.run(); return null; });
    }

}
