package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
public interface StoragePrototype {
    @Key @Filterable UniqueId key();
    @Nullable ImmutableList<Product> productList();
    @Nullable ImmutableMap<String, Product> productMapByName();
    @Nullable ImmutableMap<UniqueId, Product> productMapByKey();
    @Nullable ImmutableSet<Product> productSet();
}
