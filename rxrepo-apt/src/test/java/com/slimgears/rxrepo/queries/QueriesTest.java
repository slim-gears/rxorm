package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.query.QueryInfo;
import com.slimgears.rxrepo.util.Queries;
import io.reactivex.Observable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class QueriesTest {
    @Test
    public void testQuery() {
        QueryInfo<TestKey, TestEntity, TestEntity> queryInfo = QueryInfo.<TestKey, TestEntity, TestEntity>builder()
                .metaClass(TestEntity.metaClass)
                .predicate(TestEntity.$.text.contains("y 5")
                        .and(TestEntity.$.number.betweenExclusive(10, 124))
                        .and(TestEntity.$.refEntity.id.lessOrEq(8)))
                .skip(3L)
                .limit(5L)
                .sortDescending(TestEntity.$.text)
                .sortAscending(TestEntity.$.refEntity.id)
                .build();

        List<TestEntity> stringList = createTestEntities(1000)
                .compose(Queries.applyQuery(queryInfo))
                .doOnNext(System.out::println)
                .toList()
                .blockingGet();

        Assert.assertEquals(5, stringList.size());
    }

    private static Observable<TestEntity> createTestEntities(int count) {
        return Observable.range(0, count)
                .map(i -> TestEntity.builder()
                        .number(i)
                        .keyName("Key " + i)
                        .text("Entity " + i)
                        .refEntity(TestRefEntity
                                .builder()
                                .text("Description " + i % 10)
                                .id(i % 10)
                                .build())
                        .refEntities(Collections.emptyList())
                        .build());
    }
}
