package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.PropertyMeta;

import java.util.function.Function;
import java.util.stream.Stream;

public interface SqlAssignmentGenerator {
    <T> Function<PropertyMeta<T, ?>, Stream<String>> toAssignment(T object, ReferenceResolver referenceResolver);
}
