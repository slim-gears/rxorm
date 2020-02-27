package com.slimgears.rxrepo.util;

import io.reactivex.Observable;

public interface SchedulingProvider {
    <T> Observable<T> applyScheduler(Observable<T> observable);
}
