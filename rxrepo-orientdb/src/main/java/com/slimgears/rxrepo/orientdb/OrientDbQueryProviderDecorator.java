package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.AbstractQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;

public class OrientDbQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private OrientDbQueryProviderDecorator(QueryProvider upstream) {
        super(upstream);
    }

    static QueryProvider.Decorator decorator() {
        return OrientDbQueryProviderDecorator::new;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return underlyingProvider.liveQuery(QueryInfo.<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .build())
                .compose(Notifications.applyQuery(query));
    }
}
