package com.slimgears.rxrepo.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.annotations.EntityModel;
import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;

@EntityModel
public interface StorageEntity {
    @Key @Filterable UniqueId key();
    @Nullable ImmutableList<Product> productList();
    @Nullable ImmutableMap<String, Product> productMapByName();
    @Nullable ImmutableMap<UniqueId, Product> productMapByKey();
    @Nullable ImmutableSet<Product> productSet();
}
