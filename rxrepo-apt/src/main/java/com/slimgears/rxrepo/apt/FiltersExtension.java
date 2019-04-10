package com.slimgears.rxrepo.apt;

import com.google.auto.service.AutoService;
import com.slimgears.apt.data.AnnotationInfo;
import com.slimgears.apt.data.TypeInfo;
import com.slimgears.util.autovalue.apt.Context;
import com.slimgears.util.autovalue.apt.PropertyInfo;
import com.slimgears.util.autovalue.apt.extensions.Extension;

import javax.annotation.processing.SupportedAnnotationTypes;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@AutoService(Extension.class)
@SupportedAnnotationTypes("com.slimgears.rxrepo.annotations.UseFilters")
public class FiltersExtension implements Extension {
    @Override
    public String generateClassBody(Context context) {
        Collection<PropertyInfo> filterableProperties = context.properties()
                .stream()
                .filter(hasAnnotationOfType("com.slimgears.rxrepo.annotations.Filterable"))
                .collect(Collectors.toList());

        boolean isSearchable = context.properties()
                .stream()
                .anyMatch(hasAnnotationOfType("com.slimgears.rxrepo.annotations.Searchable"));

        return (!filterableProperties.isEmpty() || isSearchable)
                ? context.evaluatorForResource("filter-body.java.vm")
                .variable("filterableProperties", filterableProperties)
                .variable("isSearchable", isSearchable)
                .evaluate()
                : "";
    }

    private Predicate<AnnotationInfo> isAnnotationOfType(String typeName) {
        TypeInfo typeInfo = TypeInfo.of(typeName);
        return annotationInfo -> annotationInfo.type().equals(typeInfo);
    }

    private Predicate<PropertyInfo> hasAnnotationOfType(String typeName) {
        return propertyInfo -> propertyInfo.annotations().stream().anyMatch(isAnnotationOfType(typeName));
    }
}
