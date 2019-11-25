package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;

@AutoValuePrototype
@UseFilters
@UseCopyAnnotator
public interface TestRefEntityPrototype {
    @Filterable @Key int id();
    @Filterable String text();
    @Filterable @Nullable TestRefEntity testRefEntity();
}
