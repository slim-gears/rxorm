package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.util.Expressions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;

public class ExpressionsTest {
    private final Product product1 = Product.builder()
            .key(ProductKey.create(0))
                .name("Product 0")
                .price(9)
                .inventory(Inventory.builder().name("Inventory 1").id(1).build())
            .build();

    private final Product product2 = Product.builder()
            .key(ProductKey.create(10))
                .name("Product 10")
                .price(9)
                .inventory(Inventory.builder().name("Inventory 1").id(1).build())
            .build();

    @Test
    public void testPropertyExpressionCompile() {
        String name = Expressions.compile(Product.$.name).apply(product1);
        Assert.assertEquals(product1.name(), name);
    }

    @Test
    public void testReferencePropertyExpressionCompile() {
        ObjectExpression<Product, String> exp = Product.$.inventory.name;
        String name = Expressions.compile(exp).apply(product1);
        Assert.assertEquals(name, Objects.requireNonNull(product1.inventory()).name());
    }

    @Test
    public void testComparisonExpression() {
        Function<Product, Boolean> exp = Expressions.compile(Product.$.name.length().eq(Product.$.price));
        Assert.assertTrue(exp.apply(product1));
        Assert.assertFalse(exp.apply(product2));
    }

    @Test
    public void testMathExpression() {
        Function<Product, Integer> exp = Expressions.compile(Product.$.key.id.add(Product.$.price).mul(100).div(5));
        Assert.assertEquals(Integer.valueOf(180), exp.apply(product1));
        Assert.assertEquals(Integer.valueOf(380), exp.apply(product2));
    }
}
