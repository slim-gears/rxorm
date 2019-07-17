package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.query.provider.SortingInfos;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.ObservableTransformer;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NotificationsToListTransformer<K, T extends HasMetaClassWithKey<K, T>> implements ObservableTransformer<List<Notification<T>>, List<T>> {
    private final @Nullable Long limit;
    private final AtomicLong firstItemIndex;
    private final AtomicReference<T> firstItem = new AtomicReference<>();
    private final Comparator<T> comparator;
    private final Map<K, T> map = Collections.synchronizedMap(new HashMap<>());


    private NotificationsToListTransformer(ImmutableList<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos,
                                           @Nullable Long limit,
                                           AtomicLong firstItemIndex) {
        this.limit = limit;
        this.firstItemIndex = firstItemIndex;
        this.comparator = Optional
                .ofNullable(SortingInfos.toComparator(sortingInfos))
                .orElseGet(() -> Comparator.<T, String>comparing((item -> item.metaClass().keyProperty().getValue(item).toString())));
    }

    public static <K, T extends HasMetaClassWithKey<K, T>> NotificationsToListTransformer<K, T> create(
            ImmutableList<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos,
            @Nullable Long limit,
            AtomicLong firstItemIndex) {
        return new NotificationsToListTransformer<>(sortingInfos, limit, firstItemIndex);
    }

    public static <K, S extends HasMetaClassWithKey<K, S>> NotificationsToListTransformer<K, S> create(
            ImmutableList<SortingInfo<S, ?, ? extends Comparable<?>>> sortingInfos,
            @Nullable Long limit) {
        return create(sortingInfos, limit, new AtomicLong());
    }

    @Override
    public ObservableSource<List<T>> apply(Observable<List<Notification<T>>> src) {
        return src
                .doOnNext(this::updateMap)
                .map(n -> toList());
    }

    private ImmutableList<T> toList() {
        return map.values()
                .stream()
                .sorted(comparator)
                .collect(ImmutableList.toImmutableList());
    }

    private void updateMap(List<Notification<T>> notifications) {
        notifications
                .stream()
                .peek(this::updateStartIndex)
                .forEach(this::onNotification);

        removeBeforeFirst();
        removeAfterLast();
        updateFirst();
    }

    private void updateFirst() {
        map.values()
                .stream()
                .min(comparator)
                .ifPresent(firstItem::set);
    }

    private void removeAfterLast() {
        Optional.ofNullable(limit)
                .map(l -> map.values()
                        .stream()
                        .sorted(comparator)
                        .skip(l))
                .orElse(Stream.empty())
                .map(val -> val.metaClass().keyProperty().getValue(val))
                .collect(Collectors.toList())
                .forEach(map::remove);
    }

    private void removeBeforeFirst() {
        Optional.ofNullable(firstItem.get())
                .map(m -> map.values()
                        .stream()
                        .filter(val -> comparator.compare(m, val) > 0))
                .orElse(Stream.empty())
                .map(val -> val.metaClass().keyProperty().getValue(val))
                .collect(Collectors.toList())
                .forEach(map::remove);
    }

    private void updateStartIndex(Notification<T> notification) {
        if (notification.isDelete() && isBeforeFirst(notification.oldValue())) {
            firstItemIndex.decrementAndGet();
        } else if (notification.isCreate() && isBeforeFirst(notification.newValue())) {
            firstItemIndex.incrementAndGet();
        } else if (notification.isModify()) {
            if (isBeforeFirst(notification.newValue()) && !isBeforeFirst(notification.oldValue())) {
                firstItemIndex.incrementAndGet();
            } else if (!isBeforeFirst(notification.newValue()) && isBeforeFirst(notification.oldValue())) {
                firstItemIndex.decrementAndGet();
            }
        }
    }

    private boolean isBeforeFirst(T item) {
        return Optional.ofNullable(firstItem.get())
                .map(fi -> comparator.compare(fi, item) < 0)
                .orElse(false);
    }

    private void onNotification(Notification<T> notification) {
        if (notification.isDelete()) {
            Optional.ofNullable(notification.oldValue())
                    .map(val -> val.metaClass().keyProperty().getValue(val))
                    .ifPresent(map::remove);
        } else {
            Optional.ofNullable(notification.newValue())
                    .map(val -> val.metaClass().keyProperty().getValue(val))
                    .ifPresent(key -> map.put(key, notification.newValue()));
        }
    }
}
