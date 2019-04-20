package com.slimgears.rxrepo.sql;

import java.util.Arrays;
import java.util.stream.Collectors;

public class StatementUtils {
    public static String concat(String... clauses) {
        return Arrays
                .stream(clauses).filter(c -> !c.isEmpty())
                .collect(Collectors.joining(" "));
    }
}
