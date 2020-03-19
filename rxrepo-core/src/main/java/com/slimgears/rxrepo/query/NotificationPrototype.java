package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Function;

@PrototypeWithBuilder
public interface NotificationPrototype<T> {
    @Nullable T oldValue();
    @Nullable T newValue();
    @Nullable Long sequenceNumber();

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
                Optional.ofNullable(newValue()).map(mapper).orElse(null),
                sequenceNumber());
    }

    static <T> NotificationPrototype<T> ofCreated(T object, @Nullable Long sequenceNum) {
        return ofModified(null, object, sequenceNum);
    }

    static <T> NotificationPrototype<T> ofDeleted(T object, @Nullable Long sequenceNum) {
        return ofModified(object, null, sequenceNum);
    }

    static <T> NotificationPrototype<T> ofModified(T oldObject, T newObject, @Nullable Long sequenceNum) {
        return Notification.create(oldObject, newObject, sequenceNum);
    }

    static <T> NotificationPrototype<T> ofCreated(T object) {
        return ofCreated(object, null);
    }

    static <T> NotificationPrototype<T> ofDeleted(T object) {
        return ofDeleted(object, null);
    }

    static <T> NotificationPrototype<T> ofModified(T oldObject, T newObject) {
        return Notification.create(oldObject, newObject, null);
    }
}
