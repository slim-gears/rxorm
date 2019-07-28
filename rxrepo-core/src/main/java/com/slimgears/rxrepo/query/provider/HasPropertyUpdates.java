package com.slimgears.rxrepo.query.provider;

import com.google.common.collect.ImmutableList;

public interface HasPropertyUpdates<S> {
    ImmutableList<PropertyUpdateInfo<S, ?, ?>> propertyUpdates();
    ImmutableList<CollectionPropertyUpdateInfo<S, ?, ?, ?>> collectionPropertyUpdates();
}
