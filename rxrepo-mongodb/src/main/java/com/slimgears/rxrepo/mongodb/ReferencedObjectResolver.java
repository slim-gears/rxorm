package com.slimgears.rxrepo.mongodb;

import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface ReferencedObjectResolver {
    <K, S> S resolve(MetaClassWithKey<K, S> metaClass, K key);
}
