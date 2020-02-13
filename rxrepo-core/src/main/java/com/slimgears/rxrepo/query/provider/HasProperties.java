package com.slimgears.rxrepo.query.provider;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.util.autovalue.annotations.HasSelf;

import java.util.Arrays;

public interface HasProperties<T> {
    ImmutableSet<PropertyExpression<T, ?, ?>> properties();

    interface Builder<_B extends Builder<_B, T>, T> extends HasSelf<_B> {
        ImmutableSet.Builder<PropertyExpression<T, ?, ?>> propertiesBuilder();

        default <V> _B property(PropertyExpression<T, ?, V> property) {
            propertiesBuilder().add(property);
            return self();
        }

        default _B properties(PropertyExpression<T, ?, ?>... properties) {
            propertiesBuilder().addAll(Arrays.asList(properties));
            return self();
        }

    }
}
