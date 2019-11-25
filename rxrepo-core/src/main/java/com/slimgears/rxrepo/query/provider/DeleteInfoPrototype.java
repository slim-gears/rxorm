package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;

@SuppressWarnings("WeakerAccess")
@PrototypeWithBuilder
public interface DeleteInfoPrototype<K, S> extends
        HasEntityMeta<K, S>,
        HasPredicate<S>,
        HasLimit {
}
