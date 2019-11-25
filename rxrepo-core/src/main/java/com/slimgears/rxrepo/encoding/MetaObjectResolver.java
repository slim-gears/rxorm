package com.slimgears.rxrepo.encoding;

import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;

public interface MetaObjectResolver {
    <K, S> Maybe<S> resolve(MetaClassWithKey<K, S> metaClass, K key);
}
