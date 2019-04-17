package com.slimgears.rxrepo.orientdb;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
public interface ProductKeyPrototype {
    @Filterable @JsonProperty int id();
}
