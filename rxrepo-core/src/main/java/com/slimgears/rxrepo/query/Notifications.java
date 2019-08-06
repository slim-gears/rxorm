package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Maybe;
import io.reactivex.ObservableTransformer;
import io.reactivex.functions.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Notifications {
    private final static Logger log = LoggerFactory.getLogger(Notifications.class);
    public static <K, S extends HasMetaClassWithKey<K, S>> ObservableTransformer<List<Notification<S>>, List<S>> toList(
            ImmutableList<SortingInfo<S, ?, ? extends Comparable<?>>> sortingInfos,
            @Nullable Long limit) {
        return NotificationsToListTransformer.create(sortingInfos, limit);
    }

    public static <K, S extends HasMetaClassWithKey<K, S>> ObservableTransformer<List<Notification<S>>, List<S>> toList(QueryInfo<K, S, S> queryInfo, AtomicLong count) {
        return toList(queryInfo.sorting(), queryInfo.limit());
    }

    public static <K, S extends HasMetaClassWithKey<K, S>> QueryTransformer<K, S, S, List<S>> toList() {
        return (queryInfo, count) -> toList(queryInfo.sorting(), queryInfo.limit());
    }

    public static <K, S extends HasMetaClassWithKey<K, S>> ObservableTransformer<Notification<S>, Notification<S>> filter(ObjectExpression<S, Boolean> predicate) {
        if (predicate == null) {
            return src -> src;
        }

        Predicate<S> compiledPredicate = Expressions.compileRxPredicate(predicate);
        return src -> src
                .flatMapMaybe(notification -> {
                    if (notification.isCreate()) {
                        if (compiledPredicate.test(notification.newValue())) {
                            return Maybe.just(Notification.ofCreated(notification.newValue()));
                        }
                    } else if (notification.isDelete()) {
                        if (compiledPredicate.test(notification.oldValue())) {
                            return Maybe.just(Notification.ofDeleted(notification.oldValue()));
                        }
                    } else {
                        boolean oldMatch = compiledPredicate.test(notification.oldValue());
                        boolean newMatch = compiledPredicate.test(notification.newValue());
                        if (oldMatch && !newMatch) {
                            return Maybe.just(Notification.ofDeleted(notification.oldValue()));
                        } else if (!oldMatch && newMatch) {
                            return Maybe.just(Notification.ofCreated(notification.newValue()));
                        } else if (oldMatch) {
                            return Maybe.just(notification);
                        }
                    }
                    return Maybe.empty();
                });
    }

    public static <K, S extends HasMetaClassWithKey<K, S>, T> ObservableTransformer<Notification<S>, Notification<T>> applyQuery(QueryInfo<K, S, T> query) {
        return src -> src
                .doOnNext(n -> log.debug("Notification: {}", n))
                .compose(filter(query.predicate()))
                .compose(map(query.mapping()))
                .compose(fieldsFilter(query.properties()));
    }

    private static <K, S extends HasMetaClassWithKey<K, S>, T> ObservableTransformer<Notification<S>, Notification<T>> map(ObjectExpression<S, T> projection) {
        java.util.function.Function<S, T> mapper = Expressions.compile(projection);
        return src -> src.map(n -> n.map(mapper));
    }

    private static <T> ObservableTransformer<Notification<T>, Notification<T>> fieldsFilter(Collection<PropertyExpression<T, ?, ?>> properties) {
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
