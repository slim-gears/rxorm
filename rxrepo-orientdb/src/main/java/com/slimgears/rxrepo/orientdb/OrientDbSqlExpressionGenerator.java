package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.sql.DefaultSqlExpressionGenerator;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;

public class OrientDbSqlExpressionGenerator extends DefaultSqlExpressionGenerator {
    @Override
    protected ExpressionTextGenerator.Builder createBuilder() {
        return super.createBuilder()
                .add(Expression.Type.AsString, "%s.asString()");
    }
}
