package com.slimgears.util.repository.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.util.autovalue.annotations.BuilderPrototype;

public interface HasSortingInfo<T, B extends BuilderPrototype<T, B>> {
    ImmutableList<SortingInfo<T, T, B, ?>> sorting();
}
