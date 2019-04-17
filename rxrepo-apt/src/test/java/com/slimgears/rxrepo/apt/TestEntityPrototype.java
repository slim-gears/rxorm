package com.slimgears.rxrepo.apt;

import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Reference;

import java.util.Collection;

@AutoValuePrototype
@UseFilters
interface TestEntityPrototype<T extends Comparable<T>> {
    int intNumber();
    double doubleNumber();
    String text();
    String description();
    T value();
    TestReferencedEntity<T> referencedEntity();
    Collection<String> names();
    Collection<T> values();
}
