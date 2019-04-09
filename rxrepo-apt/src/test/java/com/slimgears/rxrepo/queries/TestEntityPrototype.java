package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.Reference;

import java.util.Collection;

@AutoValuePrototype
@UseFilters
public interface TestEntityPrototype {
    @Key TestKey key();
    String text();
    int number();
    @Reference TestRefEntity refEntity();
    Collection<TestRefEntity> refEntities();
}
