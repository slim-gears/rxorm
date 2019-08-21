package com.slimgears.rxrepo.query.provider;

import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface HasEntityMeta<K, S> {
    MetaClassWithKey<K, S> metaClass();
}
