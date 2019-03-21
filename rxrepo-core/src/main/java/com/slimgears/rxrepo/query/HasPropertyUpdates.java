package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;

public interface HasPropertyUpdates<S> {
    ImmutableList<PropertyUpdateInfo<S, ?, ?>> propertyUpdates();
}
