package com.slimgears.rxrepo.util;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.*;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.functions.Functions;
import io.reactivex.subjects.CompletableSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

@SuppressWarnings("WeakerAccess")
public class Queries {
    private final static Logger log = LoggerFactory.getLogger(Queries.class);

    public static <T> Comparator<T> toComparator(HasSortingInfo<T> sortingInfo) {
        return sortingInfo.sorting()
                .stream()
                .map(Queries::toComparator)
                .reduce(Comparator::thenComparing)
                .orElse(Comparator.comparing(Object::hashCode));
    }

    public static <T> Predicate<T> toPredicate(HasPredicate<T> predicate) {
        return Expressions.compilePredicate(predicate.predicate());
    }

    public static <T> ObservableTransformer<T, T> applyFilter(HasPredicate<T> hasPredicate) {
        io.reactivex.functions.Predicate<T> predicate = Optional
                .ofNullable(hasPredicate.predicate())
                .map(Expressions::compileRxPredicate)
                .orElse(t -> true);

        return source -> source.filter(predicate);
    }

    public static <T, R> ObservableTransformer<T, R> applyMapping(HasMapping<T, R> hasMapping) {
        //noinspection unchecked
        io.reactivex.functions.Function<T, R> mapper = Optional
                .ofNullable(hasMapping.mapping())
                .map(Expressions::compileRx)
                .orElse(t -> (R)t);
        return source -> source.map(mapper);
    }

    public static <T> ObservableTransformer<T, T> applySorting(HasSortingInfo<T> hasSortingInfo) {
        Comparator<T> comparator = toComparator(hasSortingInfo);
        return source -> source.sorted(comparator);
    }

    public static <T> ObservableTransformer<T, T> applyLimit(HasPagination pagination) {
        return source -> Optional
                .ofNullable(pagination.limit())
                .map(source::take)
                .orElse(source);
    }

    public static <T> ObservableTransformer<T, T> applySkip(HasPagination pagination) {
        return source -> Optional
                .ofNullable(pagination.skip())
                .map(source::skip)
                .orElse(source);
    }

    public static <T> ObservableTransformer<T, T> applyPagination(HasPagination pagination) {
        return source -> source
                .compose(applySkip(pagination))
                .compose(applyLimit(pagination));
    }

    public static <K, S, T> ObservableTransformer<S, T> applyQuery(QueryInfo<K, S, T> query) {
        return source -> source
                .compose(applyFilter(query))
                .compose(applySorting(query))
                .compose(applyMapping(query))
                .compose(applyPagination(query));
    }

    public static <T> Observable<Notification<T>> queryAndObserve(Observable<Notification<T>> query, Observable<Notification<T>> liveQuery) {
        AtomicReference<Long> lastSeqNum = new AtomicReference<>();
        CompletableSubject queryFinished = CompletableSubject.create();
        return Observable.just(
                query.doOnNext(n -> Optional
                        .ofNullable(n.sequenceNumber())
                        .ifPresent(sn -> lastSeqNum.updateAndGet(_sn -> Optional
                                .ofNullable(_sn)
                                .map(__sn -> Math.max(__sn, sn))
                                .orElse(sn))))
                        .doOnNext(n -> log.trace("Updating last sequence number (notification num: {}, last num: {})", n.sequenceNumber(), lastSeqNum.get()))
                        .concatWith(Observable
                                .just(Notification.<T>create())
                                .doOnSubscribe(d -> queryFinished.onComplete())),
                liveQuery.compose(bufferUntil(queryFinished))
                        .filter(n -> Optional
                                .ofNullable(lastSeqNum.get())
                                .flatMap(sn -> Optional
                                        .ofNullable(n.sequenceNumber())
                                        .map(nsn -> nsn > sn))
                                .orElse(true)))
                .concatMapEager(Functions.identity());
    }

    private static <T, V extends Comparable<V>> Comparator<T> toComparator(SortingInfo<T, ?, V> sortingInfo) {
        Comparator<T> comparator = Comparator.comparing(Expressions.compile(sortingInfo.property()));
        return sortingInfo.ascending()
                ? comparator
                : comparator.reversed();
    }

    private static <T> ObservableTransformer<T, T> bufferUntil(Completable releaseBufferTrigger) {
        return src -> Observable.create(emitter -> {
            List<T> buffer = new ArrayList<>();
            AtomicBoolean triggered = new AtomicBoolean();
            Disposable triggerDisposable = releaseBufferTrigger.subscribe(() -> {
                synchronized (buffer) {
                    triggered.set(true);
                    buffer.forEach(emitter::onNext);
                    buffer.clear();
                }
            });
            Disposable sourceDisposable = src.subscribe(
                    next -> {
                        if (!triggered.get()) {
                            synchronized (buffer) {
                                if (!triggered.get()) {
                                    buffer.add(next);
                                } else {
                                    emitter.onNext(next);
                                }
                            }
                        } else {
                            emitter.onNext(next);
                        }
                    },
                    emitter::onError,
                    emitter::onComplete);
            emitter.setCancellable(() -> {
                sourceDisposable.dispose();
                triggerDisposable.dispose();
            });
        });
    }
}
