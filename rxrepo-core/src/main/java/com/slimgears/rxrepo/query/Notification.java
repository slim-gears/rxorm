package com.slimgears.rxrepo.query;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Function;

public interface Notification<T> {
    @Nullable T oldObject();
    @Nullable T newObject();

    default boolean isDelete() {
        return oldObject() != null && newObject() == null;
    }

    default boolean isModify() {
        return oldObject() != null && newObject() != null;
    }

    default boolean isCreate() {
        return oldObject() == null && newObject() != null;
    }

    default <R> Notification<R> map(Function<T, R> mapper) {
        return Notification.ofModified(
                Optional.ofNullable(oldObject()).map(mapper).orElse(null),
                Optional.ofNullable(newObject()).map(mapper).orElse(null));
    }

    static <T> Notification<T> ofCreated(T object) {
        return ofModified(null, object);
    }

    static <T> Notification<T> ofDeleted(T object) {
        return ofModified(object, null);
    }

    static <T> Notification<T> ofModified(T oldObject, T newObject) {
        return new Notification<T>() {
            @Nullable
            @Override
            public T oldObject() {
                return oldObject;
            }

            @Nullable
            @Override
            public T newObject() {
                return newObject;
            }
        };
    }
}
