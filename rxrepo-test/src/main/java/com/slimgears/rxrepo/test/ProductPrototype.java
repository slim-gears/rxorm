package com.slimgears.rxrepo.test;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.rxrepo.annotations.UseFilters;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
@UseFilters
public interface ProductPrototype {
    enum Type {
        ConsumerElectronics,
        ComputeHardware,
        ComputerSoftware
    }

    @Key @Filterable UniqueId key();
    @Nullable @Filterable @Searchable String name();
    @Nullable @Searchable Type type();
    @Filterable @Nullable @Searchable Inventory inventory();
    int price();
    @Nullable List<String> aliases();
    @Nullable List<UniqueId> relatedIds();
    @Nullable Vendor vendor();
    @Nullable Date productionDate();
}
