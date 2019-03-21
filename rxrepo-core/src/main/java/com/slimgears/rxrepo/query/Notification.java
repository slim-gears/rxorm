package com.slimgears.rxrepo.query;

import javax.annotation.Nullable;

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
