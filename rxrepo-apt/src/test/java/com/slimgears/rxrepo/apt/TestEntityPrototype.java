package com.slimgears.rxrepo.apt;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Reference;
import com.slimgears.rxrepo.annotations.AutoValueExpressions;

import java.util.Collection;

@AutoValuePrototype
@AutoValueExpressions
interface TestEntityPrototype<T extends Comparable<T>> {
    int intNumber();
    double doubleNumber();
    String text();
    String description();
    T value();
    @Reference TestReferencedEntity<T> referencedEntity();
    Collection<String> names();
    Collection<T> values();
}
