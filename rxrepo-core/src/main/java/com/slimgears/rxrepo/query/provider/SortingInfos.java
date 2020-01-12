package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.stream.Streams;

import java.util.Comparator;

@SuppressWarnings("WeakerAccess")
public class SortingInfos {
    public static <T> Comparator<T> toComparator(SortingInfo<T, ?, ? extends Comparable<?>> sortingInfo) {
        return Expressions.compileComparator(sortingInfo.property(), sortingInfo.ascending());
    }

    public static <T> Comparator<T> toComparator(Iterable<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos) {
        return Streams
                .fromIterable(sortingInfos)
                .map(SortingInfos::toComparator)
                .reduce(Comparator::thenComparing)
                .orElse(null);
    }
}
