package com.slimgears.rxrepo.jdbc;

import com.slimgears.rxrepo.sql.SqlStatement;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JdbcHelper {
    private final static Map<Class, ParamSetter<?>> paramSettersByClass = new HashMap<>();
    private final static Map<Integer, ColumnGetter<?>> columnGettersByType = new HashMap<>();
    private final static ParamSetter<?> defaultSetter = JdbcHelper::setSerializedParam;

    interface ParamSetter<T> {
        void setParam(PreparedStatement preparedStatement, int index, T val) throws SQLException;
    }

    interface ColumnGetter<T> {
        T getValue(ResultSet resultSet, int columnIndex) throws SQLException;
    }
    static {
        registerType(Types.INTEGER, PreparedStatement::setInt, ResultSet::getInt, Integer.class, int.class);
        registerType(Types.BIGINT, PreparedStatement::setLong, ResultSet::getLong, Long.class, long.class);
        registerType(Types.DOUBLE, PreparedStatement::setDouble, ResultSet::getDouble, Double.class, double.class);
        registerType(Types.FLOAT, PreparedStatement::setFloat, ResultSet::getFloat, Float.class, float.class);
        registerType(Types.REAL, PreparedStatement::setFloat, ResultSet::getFloat);
        registerType(Types.SMALLINT, PreparedStatement::setShort, ResultSet::getShort, Short.class, short.class);
        registerType(Types.TINYINT, PreparedStatement::setByte, ResultSet::getByte, Byte.class, byte.class);
        registerType(Types.NVARCHAR, PreparedStatement::setString, ResultSet::getString, String.class);
        registerType(Types.VARCHAR, PreparedStatement::setString, ResultSet::getString);
        registerType(Types.NCHAR, PreparedStatement::setString, ResultSet::getString);
        registerType(Types.CHAR, PreparedStatement::setString, ResultSet::getString);
        registerType(Types.BLOB, PreparedStatement::setBytes, ResultSet::getBytes, byte[].class);
        registerType(Types.DATE, PreparedStatement::setDate, ResultSet::getDate, Date.class);
    }

    @SafeVarargs
    private static <T> void registerType(int type, ParamSetter<T> setter, ColumnGetter<T> getter, Class<T>... classes) {
        columnGettersByType.put(type, getter);
        Arrays.asList(classes).forEach(cls -> paramSettersByClass.put(cls, setter));
    }

    private static void setSerializedParam(PreparedStatement preparedStatement, int index, Object val) {
        throw new RuntimeException("Type is not supported: " + val.getClass());
    }

    public static PreparedStatement prepareStatement(Connection connection, SqlStatement statement) {
        return prepareStatement(() -> connection.prepareStatement(statement.statement()), statement.args());
    }

    public static PreparedStatement prepareStatement(Callable<PreparedStatement> supplier, Object[] params) {
        try {
            PreparedStatement preparedStatement = supplier.call();
            setParams(preparedStatement, params);
            return preparedStatement;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static void setParams(PreparedStatement preparedStatement, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; ++i) {
            Object param = Optional.ofNullable(params[i]).orElse("NULL");
            Class paramClass = param.getClass();
            ParamSetter<Object> setter = Optional
                    .ofNullable(paramSettersByClass.get(paramClass))
                    .map(ParamSetter.class::cast)
                    .orElse(defaultSetter);
            setter.setParam(preparedStatement, i + 1, param);
        }
    }

    public static Stream<ResultSet> toStream(ResultSet resultSet) {
        return StreamSupport.stream(new ResultSetSpliterator(resultSet), false);
    }

    public static Iterator<ResultSet> toIterator(ResultSet resultSet) {
        return Spliterators.iterator(new ResultSetSpliterator(resultSet));
    }

    static class ResultSetSpliterator extends Spliterators.AbstractSpliterator<ResultSet> {
        private final ResultSet resultSet;

        protected ResultSetSpliterator(ResultSet resultSet) {
            super(Long.MAX_VALUE, 0);
            this.resultSet = resultSet;
        }

        @Override
        public boolean tryAdvance(Consumer<? super ResultSet> action) {
            try {
                if (resultSet.next()) {
                    action.accept(resultSet);
                    return true;
                } else {
                    return false;
                }
            } catch (SQLException e) {
                return false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getColumnValue(ResultSet resultSet, int columnType, int columnIndex) throws SQLException {
        return (T)Optional.ofNullable(columnGettersByType.get(columnType))
                .orElseThrow(() -> new RuntimeException("Not supported type: " + columnType))
                .getValue(resultSet, columnIndex);
    }
}
