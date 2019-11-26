package com.slimgears.rxrepo.apt;

import com.slimgears.apt.data.TypeInfo;
import com.slimgears.util.autovalue.apt.PropertyInfo;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import java.util.Objects;
import java.util.Optional;

class ExtensionUtils {
    static boolean hasInnerClass(PropertyInfo property, String name) {
        return hasInnerClass(null, null, property, name);
    }

    static boolean hasInnerClass(TypeInfo sourceClass, TypeInfo targetClass, PropertyInfo property, String name) {
        return classHasPropertyOfItself(sourceClass, targetClass, property) ||
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

    private static boolean classHasPropertyOfItself(TypeInfo sourceClass, TypeInfo targetClass, PropertyInfo property) {
        if(sourceClass == null || targetClass == null) {
            return false;
        }
        boolean hasNoPackage = Optional.of(property.type()).map(type -> Objects.equals(type.fullName(), type.simpleName())).orElse(false);
        return hasNoPackage && (property.type().name().equals(targetClass.simpleName()) || property.type().name().equals(sourceClass.simpleName()));
    }
}
