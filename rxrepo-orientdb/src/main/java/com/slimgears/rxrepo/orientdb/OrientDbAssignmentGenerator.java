package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.sql.ReferenceResolver;
import com.slimgears.rxrepo.sql.SqlExpressionGenerator;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

import java.util.Collection;
import java.util.Map;

class OrientDbAssignmentGenerator extends DefaultSqlAssignmentGenerator {
    OrientDbAssignmentGenerator(SqlExpressionGenerator sqlExpressionGenerator) {
        super(sqlExpressionGenerator);
    }

    @Override
    public <K, T> Collection<Map.Entry<String, String>> toAssignment(
            MetaClassWithKey<K, T> metaClass,
            T entity,
            ReferenceResolver referenceResolver) {
        return super.toAssignment(metaClass, entity, referenceResolver);
    }

//    private String sequenceNumberAssignment() {
//        return MoreStrings.format("{} = sequence('{}').next()",
//                OrientDbQueryProvider.sequenceNumField,
//                OrientDbSchemaProvider.sequenceName);
//
//    }
}
