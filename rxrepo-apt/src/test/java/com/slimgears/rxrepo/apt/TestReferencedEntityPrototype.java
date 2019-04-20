package com.slimgears.rxrepo.apt;

import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;

@AutoValuePrototype
@UseFilters
interface TestReferencedEntityPrototype<T> {
    @Key int id();
    String text();
    String description();
    T value();
}
