package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.query.provider.SortingInfos;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.ObservableTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NotificationsToSlidingListTransformer<K, T> implements ObservableTransformer<List<Notification<T>>, List<T>> {
    private final static Logger log = LoggerFactory.getLogger(NotificationsToSlidingListTransformer.class);
    private final @Nullable Long limit;
    private final AtomicLong firstItemIndex;
    private final AtomicReference<T> firstItem = new AtomicReference<>();
    private final Comparator<T> comparator;
    private final Map<K, T> map = Collections.synchronizedMap(new HashMap<>());
    private final MetaClassWithKey<K, T> metaClass;


    private NotificationsToSlidingListTransformer(MetaClassWithKey<K, T> metaClass,
                                           ImmutableList<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos,
                                           @Nullable Long limit,
                                           AtomicLong firstItemIndex) {
        log.trace("Creating instance of list transformer for {}", metaClass.simpleName());
        this.metaClass = metaClass;
        this.limit = limit;
        this.firstItemIndex = firstItemIndex;
        this.comparator = Optional
                .ofNullable(SortingInfos.toComparator(sortingInfos))
                .orElseThrow(() -> new IllegalArgumentException("Query with sorting is expected"));
    }

    public static <K, T> NotificationsToSlidingListTransformer<K, T> create(
            MetaClassWithKey<K, T> metaClass,
            ImmutableList<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos,
            @Nullable Long limit,
            AtomicLong firstItemIndex) {
        return new NotificationsToSlidingListTransformer<>(metaClass, sortingInfos, limit, firstItemIndex);
    }

    public static <K, S> NotificationsToSlidingListTransformer<K, S> create(
            MetaClassWithKey<K, S> metaClass,
            ImmutableList<SortingInfo<S, ?, ? extends Comparable<?>>> sortingInfos,
            @Nullable Long limit) {
        return create(metaClass, sortingInfos, limit, new AtomicLong());
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
                .ifPresent(item -> {
                    log.trace("First item set: {}", item);
                    firstItem.set(item);
                });
    }

    private void removeAfterLast() {
        Optional.ofNullable(limit)
                .map(l -> map.values()
                        .stream()
                        .sorted(comparator)
                        .skip(l))
                .orElse(Stream.empty())
                .map(val -> metaClass.keyProperty().getValue(val))
                .collect(Collectors.toList())
                .forEach(map::remove);
    }

    private void removeBeforeFirst() {
        log.trace("Trying to remove item before first ({})", firstItem.get());
        Optional.ofNullable(firstItem.get())
                .map(m -> map.values()
                        .stream()
                        .peek(val -> log.trace("Comparing items: (firstItem: {}, currentItem: {}, result: {})", m, val, comparator.compare(m, val)))
                        .filter(val -> comparator.compare(m, val) > 0))
                .orElse(Stream.empty())
                .map(val -> metaClass.keyProperty().getValue(val))
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
                    .map(val -> metaClass.keyProperty().getValue(val))
                    .ifPresent(map::remove);
        } else {
            Optional.ofNullable(notification.newValue())
                    .map(val -> metaClass.keyProperty().getValue(val))
                    .ifPresent(key -> map.put(key, notification.newValue()));
        }
    }
}
