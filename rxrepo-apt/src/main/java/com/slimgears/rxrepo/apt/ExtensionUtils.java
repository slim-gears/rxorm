package com.slimgears.rxrepo.apt;

import com.slimgears.apt.data.TypeInfo;
import com.slimgears.util.autovalue.apt.PropertyInfo;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import java.util.Optional;

class ExtensionUtils {
    static boolean hasInnerClass(TypeInfo sourceClass, PropertyInfo property, String name) {
        return  classHasPropertyOfItself(sourceClass, property) ||
                Optional.of(property.propertyType())
                .flatMap(Optionals.ofType(DeclaredType.class))
                .map(DeclaredType::asElement)
                .flatMap(Optionals.ofType(TypeElement.class))
                .map(te -> te.getEnclosedElements()
                        .stream()
                        .flatMap(Streams.ofType(TypeElement.class))
                        .anyMatch(el -> name.equals(el.getSimpleName().toString())))
                .orElse(false);
    }

    private static boolean classHasPropertyOfItself(TypeInfo sourceClass, PropertyInfo property) {
        return Optional.ofNullable(sourceClass).map(TypeInfo::simpleName).map(classType -> {
            classType = removePrototypeFromTypeName(classType);
            String propertyType = removePrototypeFromTypeName(property.type().name());
            return classType.contentEquals(propertyType);
        }).orElse(false);
    }

    private static String removePrototypeFromTypeName(String name) {
        String prototype = "Prototype";
        if (name.endsWith(prototype)) {
            return name.substring(0, name.length() - prototype.length());
        }
        return name;
    }
}
