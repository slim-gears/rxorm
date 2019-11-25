package com.slimgears.rxrepo.query.provider;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.annotations.PrototypeWithBuilder;
import com.slimgears.rxrepo.expressions.ObjectExpression;

import java.util.Optional;

@PrototypeWithBuilder
public interface QueryInfoPrototype<K, S, T> extends
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
                .map(exp -> exp.reflect().objectType())
                .orElseGet(() -> (TypeToken<T>)metaClass().asType());
    }
}
