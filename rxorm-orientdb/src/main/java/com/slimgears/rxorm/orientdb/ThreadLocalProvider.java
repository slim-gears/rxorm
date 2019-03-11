package com.slimgears.rxorm.orientdb;

import javax.inject.Provider;
import java.util.Optional;

public class ThreadLocalProvider<T> implements Provider<T> {
    private final ThreadLocal<T> instance = new ThreadLocal<>();
    private final Provider<T> provider;

    private ThreadLocalProvider(Provider<T> provider) {
        this.provider = provider;
    }

    public static <T> Provider<T> of(Provider<T> provider) {
        return new ThreadLocalProvider<>(provider);
    }

    @Override
    public T get() {
        return Optional
                .ofNullable(this.instance.get())
                .orElseGet(() -> {
                    T instance = provider.get();
                    this.instance.set(instance);
                    return instance;
                });
    }
}
