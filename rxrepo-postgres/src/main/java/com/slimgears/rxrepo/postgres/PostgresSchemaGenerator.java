package com.slimgears.rxrepo.postgres;

import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.sql.SqlStatementProvider;
import com.slimgears.rxrepo.sql.jdbc.JdbcHelper;
import com.slimgears.rxrepo.sql.jdbc.JdbcSchemaGenerator;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Safe;
import io.reactivex.Completable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

public class PostgresSchemaGenerator extends JdbcSchemaGenerator {
    private final Lazy<Connection> connection;

    public PostgresSchemaGenerator(SqlStatementExecutor statementExecutor,
                                   SqlStatementProvider statementProvider,
                                   Supplier<Connection> connection) {
        super(statementExecutor, statementProvider);
        this.connection = Lazy.of(connection);
    }

    @Override
    public Completable createDatabase() {
        return super.createDatabase();
//        return Completable.fromAction(() -> {
//            String dbName = statementProvider.databaseName();
//            if (!dbExists(dbName)) {
//                statementExecutor.executeCommand(statementProvider.forCreateSchema());
//            }
//        });
    }

    private boolean dbExists(String dbName) throws SQLException {
        return JdbcHelper.toStream(connection.get().getMetaData().getCatalogs())
                .map(Safe.ofFunction(rs -> rs.getString(1)))
                .anyMatch(dbName::equals);
    }
}
