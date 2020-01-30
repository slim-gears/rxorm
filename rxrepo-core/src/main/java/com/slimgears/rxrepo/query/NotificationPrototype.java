package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Function;

@PrototypeWithBuilder
public interface NotificationPrototype<T> {
    @Nullable T oldValue();
    @Nullable T newValue();

    default boolean isDelete() {
        return oldValue() != null && newValue() == null;
    }

    default boolean isModify() {
        return oldValue() != null && newValue() != null;
    }

    default boolean isCreate() {
        return oldValue() == null && newValue() != null;
    }

    default boolean isEmpty() { return oldValue() == null && newValue() == null; }

    default <R> Notification<R> map(Function<T, R> mapper) {
        return Notification.ofModified(
                Optional.ofNullable(oldValue()).map(mapper).orElse(null),
                Optional.ofNullable(newValue()).map(mapper).orElse(null));
    }

    static <T> NotificationPrototype<T> ofCreated(T object) {
        return ofModified(null, object);
    }

    static <T> NotificationPrototype<T> ofDeleted(T object) {
        return ofModified(object, null);
    }

    static <T> NotificationPrototype<T> ofModified(T oldObject, T newObject) {
        return Notification.create(oldObject, newObject);
    }
}
