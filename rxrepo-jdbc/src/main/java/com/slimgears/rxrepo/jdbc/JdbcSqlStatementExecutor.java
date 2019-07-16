package com.slimgears.rxrepo.jdbc;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Completable;
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
        return notImplemented();
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return notImplemented();
    }

    @Override
    public Completable executeCommand(SqlStatement statement) {
        return notImplemented();
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return notImplemented();
    }

    private static <T> T notImplemented() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
