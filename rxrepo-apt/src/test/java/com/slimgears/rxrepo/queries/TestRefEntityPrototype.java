package com.slimgears.rxrepo.queries;

import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.rxrepo.annotations.AutoValueExpressions;

@AutoValuePrototype
@AutoValueExpressions
public interface TestRefEntityPrototype {
    @Key int id();
    String text();
}
