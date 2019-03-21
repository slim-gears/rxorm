package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;

public interface HasSortingInfo<T> {
    ImmutableList<SortingInfo<T, T, ?>> sorting();
}
