package com.slimgears.rxrepo.util;

import io.reactivex.*;

public class LockProviders {
    public static <T> ObservableTransformer<T, T> forObservable(LockProvider lockProvider) {
        return src -> Observable.defer(() -> {
            AutoCloseable lock = lockProvider.lock();
            return src.doFinally(lock::close);
        });
    }

    public static <T> MaybeTransformer<T, T> forMaybe(LockProvider lockProvider) {
        return src -> Maybe.defer(() -> {
            AutoCloseable lock = lockProvider.lock();
            return src.doFinally(lock::close);
        });
    }

    public static <T> SingleTransformer<T, T> forSingle(LockProvider lockProvider) {
        return src -> Single.defer(() -> {
            AutoCloseable lock = lockProvider.lock();
            return src.doFinally(lock::close);
        });
    }

    public static CompletableTransformer forCompletable(LockProvider lockProvider) {
        return src -> Completable.defer(() -> {
            AutoCloseable lock = lockProvider.lock();
            return src.doFinally(lock::close);
        });
    }
}
