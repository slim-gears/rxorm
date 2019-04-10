package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.rxrepo.annotations.UseExpressions;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseExpressions
public interface InventoryPrototype {
    @Key @Searchable @Filterable int id();
    @Nullable @Searchable @Filterable String name();
}
