package com.slimgears.rxrepo.sql;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.PropertyUpdateInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Objects;

import static org.mockito.Mockito.when;

public class SqlStatementProviderTest {
    private KeyEncoder keyEncoder;
    private SqlStatementProvider statementProvider;
    private SqlExpressionGenerator expressionGenerator;
    private SqlAssignmentGenerator assignmentGenerator;
    private ReferenceResolver referenceResolver;

    @Before
    public void setUp() {
        keyEncoder = DigestKeyEncoder.create();
        expressionGenerator = new DefaultSqlExpressionGenerator(keyEncoder);
        assignmentGenerator = new DefaultSqlAssignmentGenerator(expressionGenerator, keyEncoder);
        statementProvider = new DefaultSqlStatementProvider(expressionGenerator, assignmentGenerator, () -> "repository");
        referenceResolver = new DefaultSqlReferenceResolver(keyEncoder, expressionGenerator);
    }

    @Test
    public void testQueryStatementGeneration() {
        SqlStatement statement = statementProvider.forQuery(QueryInfo.<ObjectId, Product, Product>builder()
                .metaClass(Product.metaClass)
                .predicate(Product.$.name.contains("substr")
                        .and(Product.$.price.lessThan(100))
                        .and(Product.$.type.in(ProductPrototype.Type.ComputeHardware, ProductPrototype.Type.ComputerSoftware)))
                .properties(ImmutableSet.of(Product.$.name, Product.$.price, Product.$.id))
                .sortAscending(Product.$.name)
                .sortDescending(Product.$.id.id)
                .limit(100L)
                .skip(200L)
                .build());

        Assert.assertEquals(
                "select `name`, `price`, `id`, `__sequenceNum` from Product " +
                        "where (((`name` like '%' + ? + '%') and (`price` < ?)) and (`type` in (?))) " +
                        "order by `name` asc, `id` desc " +
                        "limit 100 " +
                        "skip 200", statement.statement());
        Assert.assertArrayEquals(statement.args(),
                new Object[]{"substr", 100, Arrays.asList(ProductPrototype.Type.ComputeHardware, ProductPrototype.Type.ComputerSoftware)});
    }

    @Test
    public void testQueryWithMappingStatementGeneration() {
        SqlStatement statement = statementProvider.forQuery(QueryInfo.<ObjectId, Product, Integer>builder()
                .metaClass(Product.metaClass)
                .mapping(Product.$.inventory.name.length().add(5))
                .predicate(Product.$.name.contains("substr").and(Product.$.price.lessThan(100)))
                .limit(100L)
                .skip(200L)
                .build());

        Assert.assertEquals(
                "select (LEN(`inventory`.`name`) + ?) " +
                        "from Product " +
                        "where ((`name` like '%' + ? + '%') and (`price` < ?)) " +
                        "limit 100 " +
                        "skip 200", statement.statement());
        Assert.assertArrayEquals(new Object[]{5, "substr", 100}, statement.args());
    }

    @Test
    public void testInsertOrUpdateStatementGeneration() {
        Product product = Product.builder()
                .id(ObjectId.create(200, "product"))
                .name("prd1")
                .price(30)
                .inventory(Inventory.builder()
                        .id(300)
                        .name("inv1")
                        .build())
                .type(ProductPrototype.Type.ComputeHardware)
                .build();

        SqlStatement statement = statementProvider.forInsertOrUpdate(Product.metaClass, product, referenceResolver);
        Assert.assertArrayEquals(new Object[] { 200, "prd1", ProductPrototype.Type.ComputeHardware, 30, 200}, statement.args());
        Assert.assertEquals(
                "update Product " +
                        "set `id` = ?, `name` = ?, `inventory` = (#31:23), `type` = ?, `price` = ? " +
                        "upsert " +
                        "return after " +
                        "where (`id` = ?)", statement.statement());
    }

    @Test
    public void testDeleteStatementGeneration() {
        SqlStatement statement = statementProvider.forDelete(DeleteInfo.<ObjectId, Product>builder()
                .metaClass(Product.metaClass)
                .limit(100L)
                .predicate(Product.$.name.greaterOrEqual("product1"))
                .build());
        Assert.assertEquals("delete from Product where (not (`name` < ?)) limit 100", statement.statement());
        Assert.assertArrayEquals(new Object[]{"product1"}, statement.args());
    }

    @Test
    public void testUpdateStatementGeneration() {
        SqlStatement statement = statementProvider.forUpdate(UpdateInfo.<ObjectId, Product>builder()
                .metaClass(Product.metaClass)
                .propertyUpdatesAdd(PropertyUpdateInfo.create(Product.$.name, Product.$.name.concat("aa")))
                .predicate(Product.$.name.contains("bbb"))
                .limit(100L)
                .build());
        Assert.assertEquals(
                "update Product " +
                        "set `name` = (`name` + ?) " +
                        "where (`name` like '%' + ? + '%') " +
                        "limit 100", statement.statement());
        Assert.assertArrayEquals(new Object[]{"aa", "bbb"}, statement.args());
    }

    @Test
    public void testCreateTableStatementGeneration() {
        SqlStatement stat1 = statementProvider.forCreateTable(Inventory.metaClass);
        SqlStatement stat2 = statementProvider.forCreateTable(Product.metaClass);
        String statement = stat1.statement() + ";\n" + stat2.statement();
        Assert.assertEquals("", statement);
    }
}
