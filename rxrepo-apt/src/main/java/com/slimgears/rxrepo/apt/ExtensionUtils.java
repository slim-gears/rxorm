package com.slimgears.rxrepo.apt;

import com.slimgears.util.autovalue.apt.PropertyInfo;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import java.util.Optional;

class ExtensionUtils {
    static boolean hasInnerClass(PropertyInfo property, String name) {
        return Optional.of(property.propertyType())
                .flatMap(Optionals.ofType(DeclaredType.class))
                .map(DeclaredType::asElement)
                .flatMap(Optionals.ofType(TypeElement.class))
                .map(te -> te.getEnclosedElements()
                        .stream()
                        .flatMap(Streams.ofType(TypeElement.class))
                        .anyMatch(el -> name.equals(el.getSimpleName().toString())))
                .orElse(false);
    }
}
