package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;

@SuppressWarnings("WeakerAccess")
@PrototypeWithBuilder
public interface UpdateInfoPrototype<K, S> extends
        HasEntityMeta<K, S>,
        HasPredicate<S>,
        HasPropertyUpdates<S>,
        HasLimit {
}
