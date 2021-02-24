package com.slimgears.rxrepo.sql.jdbc;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.AbstractSqlStatementExecutor;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlTypeMapper;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Safe;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ConcurrentModificationException;

@SuppressWarnings("UnstableApiUsage")
public class JdbcSqlStatementExecutor extends AbstractSqlStatementExecutor {
    private final Connection connection;
    private final SqlTypeMapper typeMapper;

    public JdbcSqlStatementExecutor(Connection connection, SqlTypeMapper typeMapper) {
        this.connection = connection;
        this.typeMapper = typeMapper;
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return Observable.create(emitter -> {
            logStatement("Querying", statement);
            PreparedStatement preparedStatement = JdbcHelper.prepareStatement(connection, statement);
            try {
                ResultSet resultSet = preparedStatement.executeQuery();
                try {
                    emitter.setCancellable(resultSet::close);
                    JdbcHelper.toStream(resultSet)
                            .map(rs -> JdbcResultSetPropertyResolver.create(rs, typeMapper))
                            .forEach(emitter::onNext);
                } finally {
                    resultSet.close();
                }
                emitter.onComplete();
            } catch (Throwable e) {
                emitter.onError(mapException(e));
            }
        });
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return executeQuery(statement);
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return executeQuery(statement)
                .map(p -> (int)p.getProperty("count", TypeToken.of(Integer.class)))
                .single(0);
    }

    @Override
    public Completable executeCommand(SqlStatement statement) {
        return Completable.create(emitter -> {
            try {
                PreparedStatement preparedStatement = JdbcHelper.prepareStatement(
                        connection,
                        statement);
                preparedStatement.execute();
                emitter.onComplete();
            } catch (Throwable e) {
                emitter.onError(mapException(e));
            }
        });
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return notImplemented();
    }

    private Throwable mapException(Throwable e) {
        if (e.getMessage().contains("duplicate")) {
            return new ConcurrentModificationException(e);
        }
        return e;
    }

    private static <T> T notImplemented() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
