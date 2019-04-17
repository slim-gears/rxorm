package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.Indexable;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.util.Expressions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.function.Function;

import static com.slimgears.rxrepo.filters.ComparableFilter.fromGreaterOrEqual;
import static com.slimgears.rxrepo.filters.ComparableFilter.fromLessThan;
import static com.slimgears.rxrepo.filters.StringFilter.fromContains;


public class ExpressionsTest {
    private final TestEntity testEntity1 = TestEntity.builder()
            .number(3)
            .text("Entity 1")
            .refEntity(TestRefEntity
                    .builder()
                    .text("Description 1")
                    .id(10)
                    .build())
            .keyName("Key 1")
            .refEntities(Collections.emptyList())
            .build();

    private final TestEntity testEntity2 = TestEntity.builder()
            .number(8)
            .text("Entity 2")
            .refEntity(TestRefEntity
                    .builder()
                    .text("Description 2")
                    .id(10)
                    .build())
            .keyName("Key 2")
            .refEntities(Collections.emptyList())
            .build();

    @Test
    public void testPropertyExpressionCompile() {
        String description = Expressions.compile(TestEntity.$.text).apply(testEntity1);
        Assert.assertEquals(testEntity1.text(), description);
    }

    @Test
    public void testReferencePropertyExpressionCompile() {
        ObjectExpression<TestEntity, String> exp = TestEntity.$.refEntity.text;
        String description = Expressions.compile(exp).apply(testEntity1);
        Assert.assertEquals(description, testEntity1.refEntity().text());
    }

    @Test
    public void testComparisonExpression() {
        Function<TestEntity, Boolean> exp = Expressions.compile(TestEntity.$.text.length().eq(TestEntity.$.number));
        Assert.assertFalse(exp.apply(testEntity1));
        Assert.assertTrue(exp.apply(testEntity2));
    }

    @Test
    public void testMathExpression() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.number
                        .add(TestEntity.$.text.length())
                        .add(TestEntity.$.refEntity.id)
                        .mul(100)
                        .div(5));

        Assert.assertEquals(Integer.valueOf(420), exp.apply(testEntity1));
        Assert.assertEquals(Integer.valueOf(520), exp.apply(testEntity2));
    }

    @Test
    public void testAsStringExpression() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.number.asString()
                        .concat(" - ")
                        .concat(TestEntity.$.refEntity.text)
                        .concat(" - ")
                        .concat(TestEntity.$.text));
        Assert.assertEquals("3 - Description 1 - Entity 1", exp.apply(testEntity1));
        Assert.assertEquals("8 - Description 2 - Entity 2", exp.apply(testEntity2));
    }

    @Test
    public void testFilterToExpression() {
        TestEntity.Filter filter = TestEntity.Filter.builder()
                .refEntity(TestRefEntity.Filter.builder().id(fromGreaterOrEqual(8)).build())
                .number(fromLessThan(5))
                .text(fromContains("ity 1"))
                .searchText("ity")
                .build();

        Function<TestEntity, Boolean> func = filter.toExpression(ObjectExpression.arg(TestEntity.class))
                .map(Expressions::compile)
                .orElse(e -> false);

        Assert.assertTrue(func.apply(testEntity1));
        Assert.assertFalse(func.apply(testEntity2));
    }

    @Test
    public void testAnnotationRetrieval() {
        Assert.assertTrue(TestEntity.metaClass.text.hasAnnotation(Filterable.class));
        Assert.assertTrue(TestEntity.metaClass.number.hasAnnotation(Indexable.class));
        Assert.assertFalse(TestEntity.metaClass.refEntity.hasAnnotation(Indexable.class));
    }
}
