package com.slimgears.rxrepo.test;

import com.slimgears.rxrepo.annotations.EntityModel;
import com.slimgears.util.autovalue.annotations.Key;

import javax.annotation.Nullable;

@EntityModel
public interface ManufacturerEntity {
    @Key UniqueId id();
    @Nullable String name();
}
