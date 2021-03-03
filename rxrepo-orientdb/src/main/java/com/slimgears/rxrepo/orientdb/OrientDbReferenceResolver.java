package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.sql.ReferenceResolver;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementProvider;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

class OrientDbReferenceResolver implements ReferenceResolver {
    private final SqlStatementProvider statementProvider;

    OrientDbReferenceResolver(SqlStatementProvider statementProvider) {
        this.statementProvider = statementProvider;
    }

    @Override
    public <K, S> SqlStatement toReferenceValue(MetaClassWithKey<K, S> metaClass, K key) {
        return statementProvider.forQuery(QueryInfo
                .<K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(key))
                .build());
    }
}
