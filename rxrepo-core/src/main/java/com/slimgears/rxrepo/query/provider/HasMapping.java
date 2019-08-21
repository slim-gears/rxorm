package com.slimgears.rxrepo.query.provider;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import javax.annotation.Nullable;
import java.util.Optional;

public interface HasMapping<S, T> {
    @Nullable ObjectExpression<S, T> mapping();
    @Nullable Boolean distinct();

    static <K, S, T, Q extends HasMapping<S, T> & HasEntityMeta<K, S>> TypeToken<T> objectType(Q query) {
        //noinspection unchecked
        return Optional
                .ofNullable(query.mapping())
                .map(ObjectExpression::objectType)
                .orElseGet(() -> (TypeToken)query.metaClass().asType());
    }
}
