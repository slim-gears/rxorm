package com.slimgears.rxrepo.util;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.*;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.functions.Functions;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

@SuppressWarnings("WeakerAccess")
public class Queries {
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
        return Observable.just(
                query.concatWith(Observable.just(Notification.create())),
                liveQuery)
                .concatMapEager(Functions.identity());
    }

    private static <T, V extends Comparable<V>> Comparator<T> toComparator(SortingInfo<T, ?, V> sortingInfo) {
        Comparator<T> comparator = Comparator.comparing(Expressions.compile(sortingInfo.property()));
        return sortingInfo.ascending()
                ? comparator
                : comparator.reversed();
    }
}
