package com.slimgears.rxrepo.apt;

import com.google.auto.service.AutoService;
import com.slimgears.apt.data.TypeInfo;
import com.slimgears.util.autovalue.apt.Context;
import com.slimgears.util.autovalue.apt.PropertyInfo;
import com.slimgears.util.autovalue.apt.extensions.Extension;

import javax.annotation.processing.SupportedAnnotationTypes;
import java.util.Collection;
import java.util.stream.Collectors;

@AutoService(Extension.class)
@SupportedAnnotationTypes("com.slimgears.rxrepo.annotations.UseFilters")
public class FiltersExtension implements Extension {
    @Override
    public String generateClassBody(Context context) {
        Collection<PropertyInfo> filterableProperties = context.properties()
                .stream()
                .filter(p -> p.annotations()
                        .stream()
                        .anyMatch(a -> a.type().equals(TypeInfo.of("com.slimgears.rxrepo.annotations.Filterable"))))
                .collect(Collectors.toList());

        return context.evaluatorForResource("filter-body.java.vm")
                .variable("filterableProperties", filterableProperties)
                .evaluate();
    }
}
