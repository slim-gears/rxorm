package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.query.provider.QueryInfo;
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
                        .and(TestEntity.$.number.betweenExclusive(10, 624))
                        .and(TestEntity.$.refEntity.id.lessOrEqual(8)))
                .sortAscending(TestEntity.$.refEntity.id)
                .sortDescending(TestEntity.$.text)
                .skip(3L)
                .limit(12L)
                .build();

        List<TestEntity> list = createTestEntities(1000)
                .compose(Queries.applyQuery(queryInfo))
                .toList()
                .blockingGet();

        Assert.assertEquals(12, list.size());
        Assert.assertEquals(530, list.get(3).number());
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
