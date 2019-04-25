package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

import java.util.Arrays;
import java.util.function.UnaryOperator;

public interface Repository {
    <K, T extends HasMetaClassWithKey<K, T>> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta);

    @SafeVarargs
    static Repository fromProvider(QueryProvider provider, UnaryOperator<QueryProvider>... decorators) {
        UnaryOperator<QueryProvider> decorator = Arrays
                .stream(decorators)
                .reduce((a, b) -> qp -> b.apply(a.apply(qp)))
                .orElse(UnaryOperator.identity());

        return new DefaultRepository(decorator.apply(provider));
    }
}
