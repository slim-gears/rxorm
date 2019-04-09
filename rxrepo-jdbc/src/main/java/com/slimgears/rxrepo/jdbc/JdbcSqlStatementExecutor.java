package com.slimgears.rxrepo.jdbc;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class JdbcSqlStatementExecutor implements SqlStatementExecutor {
    private final Connection connection;

    public JdbcSqlStatementExecutor(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return Observable.create(emitter -> {
            PreparedStatement preparedStatement = JdbcHelper.prepareStatement(connection, statement);
            JdbcHelper.toStream(preparedStatement.executeQuery());
        });
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return null;
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return null;
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return null;
    }
}
