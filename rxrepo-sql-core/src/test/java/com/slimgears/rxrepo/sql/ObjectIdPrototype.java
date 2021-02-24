package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
public interface ObjectIdPrototype {
    int id();
    String name();
}
