package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
public interface ProductPrototype {
    enum Type {
        ConsumerElectronics,
        ComputeHardware,
        ComputerSoftware
    }

    @Key int id();
    @Nullable String name();
    @Nullable Inventory inventory();
    @Nullable Type type();
    int price();
}
