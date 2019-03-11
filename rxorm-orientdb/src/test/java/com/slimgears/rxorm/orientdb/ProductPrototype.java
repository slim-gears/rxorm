package com.slimgears.rxorm.orientdb;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.Reference;
import com.slimgears.util.repository.annotations.AutoValueExpressions;

import javax.annotation.Nullable;

@AutoValuePrototype
@AutoValueExpressions
public interface ProductPrototype {
    @Key long id();
    @Nullable String name();
    @Reference @Nullable Inventory inventory();
}
