package com.slimgears.rxrepo.orientdb;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.rxrepo.annotations.UseExpressions;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseExpressions
public interface InventoryPrototype {
    @Key int id();
    @Nullable String name();
}
