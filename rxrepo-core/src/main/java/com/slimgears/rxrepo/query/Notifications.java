package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.query.provider.SortingInfos;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.ObservableTransformer;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Notifications {
    public static <K, T extends HasMetaClassWithKey<K, T>> ObservableTransformer<List<Notification<T>>, List<T>> toList(
            ImmutableList<SortingInfo<T, ?, ?>> sortingInfos,
            @Nullable Long limit) {

        Comparator<T> comparator = Optional
                .ofNullable(SortingInfos.toComparator(sortingInfos))
                .orElseGet(() -> Comparator.<T, String>comparing((item -> item.metaClass().keyProperty().getValue(item).toString())));

        AtomicReference<T> min = new AtomicReference<>();

        Map<K, T> map = Collections.synchronizedMap(new HashMap<>());
        return src -> src
                .doOnNext(notifications -> notifications.forEach(n -> {
                            if (n.isDelete()) {
                                Optional.ofNullable(n.oldValue())
                                        .map(val -> val.metaClass().keyProperty().getValue(val))
                                        .ifPresent(map::remove);
                            } else {
                                Optional.ofNullable(n.newValue())
                                        .map(val -> val.metaClass().keyProperty().getValue(val))
                                        .ifPresent(key -> map.put(key, n.newValue()));
                            }
                        }))
                .doOnNext(notifications -> {
                    Optional.ofNullable(min.get())
                            .map(m -> map.values()
                                    .stream()
                                    .filter(val -> comparator.compare(m, val) > 0))
                            .orElse(Stream.empty())
                            .map(val -> val.metaClass().keyProperty().getValue(val))
                            .collect(Collectors.toList())
                            .forEach(map::remove);

                    Optional.ofNullable(limit)
                            .map(l -> map.values()
                                    .stream()
                                    .sorted(comparator)
                                    .skip(l))
                            .orElse(Stream.empty())
                            .map(val -> val.metaClass().keyProperty().getValue(val))
                            .collect(Collectors.toList())
                            .forEach(map::remove);
                    map.values()
                            .stream()
                            .min(comparator)
                            .ifPresent(min::set);
                })
                .map(n -> map.values()
                        .stream()
                        .sorted(comparator)
                        .collect(ImmutableList.toImmutableList()));
    }
}
