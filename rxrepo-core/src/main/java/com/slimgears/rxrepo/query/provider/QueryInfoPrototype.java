package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.reflect.TypeToken;

import java.util.Optional;

@PrototypeWithBuilder
public interface QueryInfoPrototype<K, S extends HasMetaClassWithKey<K, S>, T> extends
        HasEntityMeta<K, S>,
        HasPredicate<S>,
        HasProperties<T>,
        HasMapping<S, T>,
        HasSortingInfo<S>,
        HasPagination {
    @SuppressWarnings("unchecked")
    default TypeToken<T> objectType() {
        return Optional
                .ofNullable(mapping())
                .map(ObjectExpression::objectType)
                .orElseGet(() -> (TypeToken<T>)metaClass().objectClass());
    }
}
