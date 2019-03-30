package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.annotations.AutoValueExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.Reference;

import javax.annotation.Nullable;

@AutoValuePrototype
@AutoValueExpressions
public interface ProductPrototype {
    @Key int id();
    @Nullable String name();
    @Reference @Nullable Inventory inventory();
    int price();
}
