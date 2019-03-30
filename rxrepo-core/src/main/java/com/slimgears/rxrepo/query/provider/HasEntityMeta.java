package com.slimgears.rxrepo.query.provider;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface HasEntityMeta<K, S extends HasMetaClassWithKey<K, S>> {
    MetaClassWithKey<K, S> metaClass();
}
