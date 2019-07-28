package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.reflect.TypeToken;

import javax.annotation.Nullable;
import java.util.Optional;

public interface HasMapping<S, T> {
    @Nullable ObjectExpression<S, T> mapping();
    @Nullable Boolean distinct();

    static <K, S extends HasMetaClassWithKey<K, S>, T, Q extends HasMapping<S, T> & HasEntityMeta<K, S>> TypeToken<T> objectType(Q query) {
        //noinspection unchecked
        return Optional
                .ofNullable(query.mapping())
                .map(ObjectExpression::objectType)
                .orElseGet(() -> (TypeToken)query.metaClass().objectClass());
    }
}
