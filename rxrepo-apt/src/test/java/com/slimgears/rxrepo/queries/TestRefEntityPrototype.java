package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;

@AutoValuePrototype
@UseFilters
public interface TestRefEntityPrototype {
    @Key int id();
    String text();
}
