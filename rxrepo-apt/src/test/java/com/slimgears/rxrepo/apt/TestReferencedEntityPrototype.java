package com.slimgears.rxrepo.apt;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.rxrepo.annotations.AutoValueExpressions;

@AutoValuePrototype
@AutoValueExpressions
interface TestReferencedEntityPrototype<T> {
    String text();
    String description();
    T value();
}
