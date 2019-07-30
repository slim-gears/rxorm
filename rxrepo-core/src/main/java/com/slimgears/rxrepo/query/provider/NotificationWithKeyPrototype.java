package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;
import com.slimgears.rxrepo.query.NotificationPrototype;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.stream.Optionals;

import java.util.Optional;

@PrototypeWithBuilder
public interface NotificationWithKeyPrototype<K, S> extends NotificationPrototype<S> {
    K key();

    default boolean isDelete() {
        return newValue() == null;
    }

    default boolean isModify() {
        return oldValue() != null && newValue() != null;
    }

    default boolean isCreate() {
        return oldValue() == null && newValue() != null;
    }

    static <K, S extends HasMetaClassWithKey<K, S>> NotificationWithKeyPrototype<K, S> ofCreated(S entity) {
        return NotificationWithKey.
                <K, S>builder()
                .key(HasMetaClassWithKey.keyOf(entity))
                .newValue(entity)
                .build();
    }

    static <K, S extends HasMetaClassWithKey<K, S>> NotificationWithKeyPrototype<K, S> ofDeleted(K key) {
        return NotificationWithKey.<K, S>builder().key(key).build();
    }

    static <K, S extends HasMetaClassWithKey<K, S>> NotificationWithKeyPrototype<K, S> ofModified(S oldValue, S newValue) {
        K key = Optionals.or(
                () -> Optional.ofNullable(oldValue),
                () -> Optional.ofNullable(newValue))
                .map(HasMetaClassWithKey::keyOf)
                .orElse(null);

        return NotificationWithKey.<K, S>builder()
                .key(key)
                .oldValue(oldValue)
                .newValue(newValue)
                .build();
    }

    static <K, S extends HasMetaClassWithKey<K, S>> NotificationWithKeyPrototype<K, S> fromNotification(NotificationPrototype<S> notification) {
        return ofModified(notification.oldValue(), notification.newValue());
    }
}
