package com.slimgears.rxrepo.util;

import io.reactivex.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Timeout {
    public static CompletableTransformer forCompletable(Duration timeout) {
        return src -> src.timeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static <T> MaybeTransformer<T, T> forMaybe(Duration timeout) {
        return src -> src.timeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static <T> ObservableTransformer<T, T> tillFirst(Duration timeout) {
        return src -> src.timeout(Observable.empty().delay(timeout.toMillis(), TimeUnit.MILLISECONDS), o -> Observable.never());
    }

    public static <T> ObservableTransformer<T, T> forObservable(Duration timeout) {
        return src -> src.timeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public static <T> SingleTransformer<T, T> forSingle(Duration timeout) {
        return src -> src.timeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
