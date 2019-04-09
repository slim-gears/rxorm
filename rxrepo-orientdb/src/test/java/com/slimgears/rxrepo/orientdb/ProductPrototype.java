package com.slimgears.rxrepo.orientdb;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.Reference;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
public interface ProductPrototype {
    @Key int id();
    @Nullable String name();
    @Reference @Nullable Inventory inventory();
    int price();
}
