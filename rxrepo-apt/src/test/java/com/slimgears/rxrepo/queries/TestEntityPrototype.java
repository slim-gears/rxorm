package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.Indexable;
import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.Reference;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import java.util.Collection;

@AutoValuePrototype
@UseFilters
@UseCopyAnnotator
public interface TestEntityPrototype {
    @Key TestKey key();
    @Indexable @Filterable String text();
    @Indexable @Filterable int number();
    @Reference @Filterable TestRefEntity refEntity();
    Collection<TestRefEntity> refEntities();
}
