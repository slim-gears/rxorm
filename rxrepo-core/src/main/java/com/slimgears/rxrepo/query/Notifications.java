package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import io.reactivex.ObservableTransformer;

import java.util.*;

public class Notifications {
    public static <K, T extends HasMetaClassWithKey<K, T>> ObservableTransformer<Notification<T>, List<T>> toList() {
        Map<K, T> map = Collections.synchronizedMap(new LinkedHashMap<>());
        return src -> src
            .doOnNext(n -> {
                if (n.isDelete()) {
                    Optional.ofNullable(n.oldValue())
                            .map(val -> val.metaClass().keyProperty().getValue(val))
                            .ifPresent(map::remove);
                } else {
                    Optional.ofNullable(n.newValue())
                            .map(val -> val.metaClass().keyProperty().getValue(val))
                            .ifPresent(key -> map.put(key, n.newValue()));
                }
            })
            .map(n -> ImmutableList.copyOf(map.values()));
    }
}
