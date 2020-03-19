package com.slimgears.rxrepo.util;

import io.reactivex.ObservableTransformer;

public interface SchedulingProvider {
    <T> ObservableTransformer<T, T> applyScope();
    <T> ObservableTransformer<T, T> applyScheduler();
}
