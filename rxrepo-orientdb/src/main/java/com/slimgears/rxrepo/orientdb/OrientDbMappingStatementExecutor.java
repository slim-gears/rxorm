package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

import static com.slimgears.rxrepo.orientdb.OrientDbObjectConverter.toOrientDb;

public class OrientDbMappingStatementExecutor implements SqlStatementExecutor {
    private final SqlStatementExecutor underlyingExecutor;

    private OrientDbMappingStatementExecutor(SqlStatementExecutor underlyingExecutor) {
        this.underlyingExecutor = underlyingExecutor;
    }

    static SqlStatementExecutor decorate(SqlStatementExecutor executor) {
        return new OrientDbMappingStatementExecutor(executor);
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return underlyingExecutor.executeQuery(toOrientDb(statement));
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return underlyingExecutor.executeCommandReturnEntries(toOrientDb(statement));
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
