package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

import java.util.function.Function;
import java.util.stream.Stream;

public interface SqlAssignmentGenerator {
    <K, T> Function<String, Stream<String>> toAssignment(MetaClassWithKey<K, T> metaClass,
                                                                                           PropertyResolver propertyResolver,
                                                                                           ReferenceResolver referenceResolver);
}
