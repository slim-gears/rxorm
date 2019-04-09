package com.slimgears.rxrepo.apt;

import com.google.auto.service.AutoService;
import com.slimgears.util.autovalue.apt.Context;
import com.slimgears.util.autovalue.apt.extensions.Extension;

import javax.annotation.processing.SupportedAnnotationTypes;

@AutoService(Extension.class)
@SupportedAnnotationTypes("com.slimgears.rxrepo.annotations.UseFilters")
public class FiltersExtension implements Extension {
    @Override
    public String generateClassBody(Context context) {
        return context.evaluateResource("filter-body.java.vm");
    }
}
