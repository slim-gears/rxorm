package com.slimgears.rxrepo.util;

import io.reactivex.*;
import io.reactivex.functions.Function;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Timeout {
    public static CompletableTransformer forCompletable(Duration timeout) {
        return src -> src
                .timeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .onErrorResumeNext(addCauseIfTimeout(timeout, Completable::error));
    }

    public static <T> MaybeTransformer<T, T> forMaybe(Duration timeout) {
        return src -> src
                .timeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .onErrorResumeNext(addCauseIfTimeout(timeout, Maybe::error));
    }

    public static <T> ObservableTransformer<T, T> tillFirst(Duration timeout) {
        return src -> src
                .timeout(Observable.empty().delay(timeout.toMillis(), TimeUnit.MILLISECONDS), o -> Observable.never())
                .onErrorResumeNext(addCauseIfTimeout(timeout, Observable::error));
    }

    public static <T> ObservableTransformer<T, T> forObservable(Duration timeout) {
        return src -> src
                .timeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .onErrorResumeNext(addCauseIfTimeout(timeout, Observable::error));
    }

    public static <T> SingleTransformer<T, T> forSingle(Duration timeout) {
        return src -> src
                .timeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .onErrorResumeNext(addCauseIfTimeout(timeout, Single::error));
    }

    private static <T> Function<Throwable, T> addCauseIfTimeout(Duration timeout, Function<Throwable, T> errorGenerator) {
        Exception timeoutException = new TimeoutException("Operation did not finish within " + timeout.toMillis() + "ms");
        return exception -> {
            if (exception instanceof TimeoutException) {
                exception.initCause(timeoutException);
            }
            return errorGenerator.apply(exception);
        };
    }
}
