package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

@AutoValuePrototype
@UseExpressions
@UseCopyAnnotator
public interface VendorPrototype {
    UniqueId id();
    String name();
}
