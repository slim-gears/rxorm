package com.slimgears.rxrepo.sql;

import com.slimgears.util.stream.Streams;

import java.util.Arrays;
import java.util.stream.Collectors;

public interface SqlStatement {
    SqlStatement empty = create("");

    String statement();
    Object[] args();

    default SqlStatement withArgs(Object... args) {
        return create(statement(), args);
    }

    static SqlStatement of(String... clauses) {
        return create(Arrays
                .stream(clauses).filter(c -> !c.isEmpty())
                .collect(Collectors.joining(" ")));
    }

    static SqlStatement empty() {
        return empty;
    }

    static SqlStatement create(String statement, Object... args) {
        return new SqlStatement() {
            @Override
            public String statement() {
                return statement;
            }

            @Override
            public Object[] args() {
                return args;
            }
        };
    }
}
