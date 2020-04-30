package com.slimgears.rxrepo.test;

import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
@UseFilters
public interface ManufacturerPrototype {
    @Key UniqueId id();
    @Nullable String name();
}
