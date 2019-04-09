package com.slimgears.rxrepo.apt;

import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;

@AutoValuePrototype
@UseFilters
interface TestReferencedEntityPrototype<T> {
    String text();
    String description();
    T value();
}
