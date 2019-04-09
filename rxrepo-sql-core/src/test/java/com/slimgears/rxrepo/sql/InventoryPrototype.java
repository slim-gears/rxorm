package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseExpressions
public interface InventoryPrototype {
    @Key int id();
    @Nullable String name();
}
