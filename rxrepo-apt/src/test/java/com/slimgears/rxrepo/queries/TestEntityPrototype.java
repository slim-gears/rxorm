package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.Indexable;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;
import java.util.Collection;

@AutoValuePrototype
@UseFilters
@UseCopyAnnotator
public interface TestEntityPrototype {
    enum TestEnum {
        Val1,
        Val2
    }

    @Key @Searchable TestKey key();
    @Indexable @Filterable @Searchable String text();
    @Indexable @Filterable int number();
    @Filterable TestRefEntity refEntity();
    Collection<TestRefEntity> refEntities();
    @Nullable String address();
    @Nullable Integer code();
    @Nullable Collection<String> col();
    @Nullable TestRefEntity optionalRefEntity();
    @Nullable TestEnum enumVal();
}
