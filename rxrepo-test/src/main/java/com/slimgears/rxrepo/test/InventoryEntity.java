package com.slimgears.rxrepo.test;

import com.slimgears.rxrepo.annotations.EntityModel;
import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;

@EntityModel
public interface InventoryEntity {
    @Key @Filterable UniqueId id();
    @Nullable @Searchable @Filterable String name();
    @Nullable Inventory inventory();
    @Nullable Manufacturer manufacturer();
}
