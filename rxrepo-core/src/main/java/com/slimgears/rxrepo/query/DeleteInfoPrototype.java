package com.slimgears.rxrepo.query;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;

@AutoValuePrototype
public interface DeleteInfoPrototype<K, S extends HasMetaClassWithKey<K, S>> extends
        HasEntityMeta<K, S>,
        HasPredicate<S>,
        HasPagination {
}
