package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryInfos;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MandatoryPropertiesQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Logger log = LoggerFactory.getLogger(MandatoryPropertiesQueryProviderDecorator.class);

    private MandatoryPropertiesQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static QueryProvider.Decorator create() {
        return MandatoryPropertiesQueryProviderDecorator::new;
    }

    @Override
    public <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
        return super.query(QueryInfos.includeMandatoryProperties(query));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(QueryInfos.includeMandatoryProperties(queryInfo), QueryInfos.includeMandatoryProperties(observeInfo));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return super.liveQuery(QueryInfos.includeMandatoryProperties(query));
    }

}
