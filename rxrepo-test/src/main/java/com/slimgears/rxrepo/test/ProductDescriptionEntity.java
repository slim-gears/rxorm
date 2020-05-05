package com.slimgears.rxrepo.test;

import com.slimgears.rxrepo.annotations.EntityModel;
import com.slimgears.rxrepo.annotations.UseExpressions;
import com.slimgears.util.autovalue.annotations.AutoValuePrototype;
import com.slimgears.util.autovalue.annotations.Key;
import com.slimgears.util.autovalue.annotations.UseCopyAnnotator;

@EntityModel
public interface ProductDescriptionEntity {
    @Key UniqueId key();
    Product product();
}
