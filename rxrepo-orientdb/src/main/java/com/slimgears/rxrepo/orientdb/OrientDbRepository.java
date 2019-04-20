package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.sql.DefaultSqlStatementProvider;
import com.slimgears.rxrepo.sql.SqlServiceFactory;

import java.util.function.Supplier;

public class OrientDbRepository {
    private static SqlServiceFactory.Builder builder(Supplier<ODatabaseDocument> sessionSupplier) {
        OrientDbSessionProvider sessionProvider = OrientDbSessionProvider.create(sessionSupplier);
        return SqlServiceFactory.builder()
                .schemaProvider(() -> new OrientDbSchemaProvider(sessionProvider))
                .statementExecutor(() -> new OrientDbStatementExecutor(sessionProvider))
                .expressionGenerator(OrientDbSqlExpressionGenerator::new)
                .assignmentGenerator(svc -> new OrientDbAssignmentGenerator(svc.expressionGenerator()))
                .statementProvider(svc -> new DefaultSqlStatementProvider(svc.expressionGenerator(), svc.assignmentGenerator(), svc.schemaProvider()))
                .referenceResolver(svc -> new OrientDbReferenceResolver(svc.statementProvider()));
    }

    public static Repository create(Supplier<ODatabaseDocument> sessionSupplier) {
        return Repository.fromProvider(builder(sessionSupplier)
                .build()
                .queryProvider());
    }
}
