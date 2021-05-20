package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.KeyEncoder;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

public class OrientDbMappingStatementExecutor implements SqlStatementExecutor {
    private final SqlStatementExecutor underlyingExecutor;
    private final OrientDbObjectConverter objectConverter;

    private OrientDbMappingStatementExecutor(SqlStatementExecutor underlyingExecutor, OrientDbObjectConverter objectConverter) {
        this.underlyingExecutor = underlyingExecutor;
        this.objectConverter = objectConverter;
    }

    static SqlStatementExecutor decorate(SqlStatementExecutor executor, KeyEncoder keyEncoder) {
        return new OrientDbMappingStatementExecutor(executor, OrientDbObjectConverter.create(keyEncoder));
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return underlyingExecutor.executeQuery(toOrientDb(statement));
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return underlyingExecutor.executeCommandReturnEntries(toOrientDb(statement));
    }

    private SqlStatement toOrientDb(SqlStatement statement) {
        return statement.mapArgs(objectConverter::toOrientDbObject);
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return underlyingExecutor.executeCommandReturnCount(toOrientDb(statement));
    }

    @Override
    public Completable executeCommand(SqlStatement statement) {
        return underlyingExecutor.executeCommand(toOrientDb(statement));
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return underlyingExecutor.executeLiveQuery(toOrientDb(statement));
    }
}
