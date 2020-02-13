package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.decorator.UpdateReferencesFirstQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;

public class OrientDbUpdateReferencesFirstQueryProviderDecorator extends UpdateReferencesFirstQueryProviderDecorator {
    private final QueryProvider underlyingProvider;

    private OrientDbUpdateReferencesFirstQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
        this.underlyingProvider = underlyingProvider;
    }

    public static Decorator create() {
        return OrientDbUpdateReferencesFirstQueryProviderDecorator::new;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return underlyingProvider.insert(metaClass, entities, recursive);
    }
}
