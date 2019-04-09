package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;

@PrototypeWithBuilder
public interface DeleteInfoPrototype<K, S extends HasMetaClassWithKey<K, S>> extends
        HasEntityMeta<K, S>,
        HasPredicate<S>,
        HasLimit {
}
