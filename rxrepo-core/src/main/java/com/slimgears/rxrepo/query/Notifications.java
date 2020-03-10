package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.ObservableTransformer;
import io.reactivex.functions.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Notifications {
    private final static Logger log = LoggerFactory.getLogger(Notifications.class);
    public static <K, S, T> ObservableTransformer<List<Notification<S>>, List<T>> toList(
            MetaClassWithKey<K, S> metaClass,
            ImmutableList<SortingInfo<S, ?, ? extends Comparable<?>>> sortingInfos,
            @Nullable ObjectExpression<S, T> mapping,
            @Nullable Long limit) {
        Function<S, T> mapper = Expressions.compile(mapping);
        ObservableTransformer<List<Notification<S>>, List<S>> transformer = NotificationsToListTransformer.create(metaClass, sortingInfos, limit);
        return src -> src
            .compose(transformer)
            .map(objects -> objects.stream().map(mapper).collect(Collectors.toList()));
    }

    private static <K, S, T> ObservableTransformer<List<Notification<S>>, List<T>> toSlidingList(
            MetaClassWithKey<K, S> metaClass,
            ImmutableList<SortingInfo<S, ?, ? extends Comparable<?>>> sortingInfos,
            @Nullable ObjectExpression<S, T> mapping,
            @Nullable Long limit) {
        Function<S, T> mapper = Expressions.compile(mapping);
        ObservableTransformer<List<Notification<S>>, List<S>> transformer =
                Optional.ofNullable(sortingInfos).map(List::size).map(s -> s > 0).orElse(false)
                        ? NotificationsToSlidingListTransformer.create(metaClass, sortingInfos, limit)
                        : NotificationsToListTransformer.create(metaClass, sortingInfos, limit);
        return src -> src
            .compose(transformer)
            .map(objects -> objects.stream().map(mapper).collect(Collectors.toList()));
    }

    public static <K, S> ObservableTransformer<List<Notification<S>>, List<S>> toList(QueryInfo<K, S, S> queryInfo, AtomicLong count) {
        return Notifications.toList(queryInfo.metaClass(), queryInfo.sorting(), queryInfo.mapping(), queryInfo.limit());
    }

    public static <K, S> ObservableTransformer<List<Notification<S>>, List<S>> toSlidingList(QueryInfo<K, S, S> queryInfo, AtomicLong count) {
        return Notifications.toSlidingList(queryInfo.metaClass(), queryInfo.sorting(), queryInfo.mapping(), queryInfo.limit());
    }

    public static <T> QueryTransformer<T, List<T>> toList() {
        return new QueryTransformer<T, List<T>>() {
            @Override
            public <K, S> ObservableTransformer<List<Notification<S>>, List<T>> transformer(QueryInfo<K, S, T> query, AtomicLong count) {
                return toList(query.metaClass(), query.sorting(), query.mapping(), query.limit());
            }
        };
    }

    public static <T> QueryTransformer<T, List<T>> toSlidingList() {
        return new QueryTransformer<T, List<T>>() {
            @Override
            public <K, S> ObservableTransformer<List<Notification<S>>, List<T>> transformer(QueryInfo<K, S, T> query, AtomicLong count) {
                return toSlidingList(query.metaClass(), query.sorting(), query.mapping(), query.limit());
            }
        };
    }

    public static <S> ObservableTransformer<Notification<S>, Notification<S>> applyFilter(ObjectExpression<S, Boolean> predicate) {
        if (predicate == null) {
            return src -> src;
        }

        Predicate<S> compiledPredicate = Expressions.compileRxPredicate(predicate);
        return src -> src
                .flatMapMaybe(notification -> {
                    if (notification.isCreate()) {
                        if (compiledPredicate.test(notification.newValue())) {
                            return Maybe.just(Notification.ofCreated(notification.newValue(), notification.generation()));
                        }
                    } else if (notification.isDelete()) {
                        if (compiledPredicate.test(notification.oldValue())) {
                            return Maybe.just(Notification.ofDeleted(notification.oldValue(), notification.generation()));
                        }
                    } else if (notification.isModify()) {
                        boolean oldMatch = compiledPredicate.test(notification.oldValue());
                        boolean newMatch = compiledPredicate.test(notification.newValue());
                        if (oldMatch && !newMatch) {
                            return Maybe.just(Notification.ofDeleted(notification.oldValue(), notification.generation()));
                        } else if (!oldMatch && newMatch) {
                            return Maybe.just(Notification.ofCreated(notification.newValue(), notification.generation()));
                        } else if (oldMatch) {
                            return Maybe.just(notification);
                        }
                    } else {
                        return Maybe.just(notification);
                    }
                    return Maybe.empty();
                });
    }

    public static <K, S, T> ObservableTransformer<Notification<S>, Notification<T>> applyQuery(QueryInfo<K, S, T> query) {
        return src -> src
                .doOnNext(n -> log.trace("Notification (before): {}", n))
                .compose(applyFilter(query.predicate()))
                .compose(applyMap(query.mapping()))
                .compose(applyFieldsFilter(query.properties()))
                .doOnNext(n -> log.trace("Notification (after): {}", n));
    }

    public static <S, T> ObservableTransformer<Notification<S>, Notification<T>> applyMap(ObjectExpression<S, T> projection) {
        java.util.function.Function<S, T> mapper = Expressions.compile(projection);
        return src -> src.map(n -> n.map(mapper));
    }

    public static <T> ObservableTransformer<Notification<T>, Notification<T>> applyFieldsFilter(Collection<PropertyExpression<T, ?, ?>> properties) {
        List<java.util.function.Function<T, ?>> propertyMetas = properties.stream()
                .map(Expressions::compile)
                .collect(Collectors.toList());

        return properties.isEmpty()
                ? src -> src
                : src -> src.filter(n -> fieldsChanged(n, propertyMetas));
    }

    private static <T> boolean fieldsChanged(Notification<T> notification, List<java.util.function.Function<T, ?>> properties) {
        if (!notification.isModify() || properties.isEmpty()) {
            return true;
        }

        return properties
                .stream()
                .anyMatch(p -> !Objects.equals(
                        p.apply(notification.oldValue()),
                        p.apply(notification.newValue())));
    }

}
