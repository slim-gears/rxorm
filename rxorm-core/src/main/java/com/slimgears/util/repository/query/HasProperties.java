package com.slimgears.util.repository.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.util.autovalue.annotations.HasSelf;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.repository.expressions.PropertyExpression;

import java.util.Arrays;

public interface HasProperties<T> {
    ImmutableList<PropertyExpression<T, ?, ?>> properties();

    interface Builder<_B extends Builder<_B, T>, T> extends HasSelf<_B> {
        ImmutableList.Builder<PropertyExpression<T, ?, ?>> propertiesBuilder();

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
