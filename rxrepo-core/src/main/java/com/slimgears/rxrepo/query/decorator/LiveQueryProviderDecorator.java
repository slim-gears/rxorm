package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LiveQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Logger log = LoggerFactory.getLogger(LiveQueryProviderDecorator.class);
    private LiveQueryProviderDecorator(QueryProvider upstream) {
        super(upstream);
    }

    public static QueryProvider.Decorator decorator() {
        return LiveQueryProviderDecorator::new;
    }

    @Override
    public <K, S extends HasMetaClassWithKey<K, S>, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return underlyingProvider.liveQuery(QueryInfo.<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .build())
                .compose(Notifications.applyQuery(query));
    }
}
