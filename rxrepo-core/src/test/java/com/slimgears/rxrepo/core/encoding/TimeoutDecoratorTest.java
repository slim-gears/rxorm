package com.slimgears.rxrepo.core.encoding;

import com.google.common.base.Stopwatch;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.decorator.OperationTimeoutQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TimeoutDecoratorTest {
    private QueryProvider queryProviderMock;
    private final QueryProvider.Decorator decorator = OperationTimeoutQueryProviderDecorator
            .create(Duration.ofMillis(500), Duration.ofMillis(500));
    private QueryProvider decoratedProvider;

    @Before
    public void setUp() {
        queryProviderMock = mock(QueryProvider.class);
        decoratedProvider = decorator.apply(queryProviderMock);
    }

    @Test
    public void testQueryTimeout() {
        when(queryProviderMock.query(any())).thenReturn(Observable.never());
        decoratedProvider.query(mock(QueryInfo.class))
                .test()
                .awaitCount(1)
                .assertError(t -> t instanceof TimeoutException);
        when(queryProviderMock.<Object, Object, Integer>query(any())).thenReturn(Observable.just(Notification.fromNewValue(1)));
        decoratedProvider.query(mock(QueryInfo.class))
                .test()
                .awaitCount(1)
                .assertValueCount(1)
                .assertComplete();
    }

    @Test
    public void testQueryAndObserveTimeout() {
        when(queryProviderMock.queryAndObserve(any(), any())).thenReturn(Observable.never());
        decoratedProvider.queryAndObserve(mock(QueryInfo.class), mock(QueryInfo.class))
                .test()
                .awaitCount(1)
                .assertError(t -> t instanceof TimeoutException);
        when(queryProviderMock.<Object, Object, Integer>queryAndObserve(any(), any()))
                .thenReturn(Observable.just(Notification.fromNewValue(1)).concatWith(Observable.never()));
        decoratedProvider.queryAndObserve(mock(QueryInfo.class), mock(QueryInfo.class))
                .test()
                .awaitCount(1)
                .assertValueCount(1)
                .assertNotComplete();
    }

    @Test
    public void testTimeout() throws InterruptedException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Completable.fromAction(() -> Thread.sleep(2000))
                .timeout(500, TimeUnit.MILLISECONDS)
                .test()
                .await()
                .assertError(TimeoutException.class);
        System.out.println("Elapsed milliseconds: " + stopwatch.elapsed().toMillis());
    }
}
