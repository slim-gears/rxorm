package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.test.Inventory;
import com.slimgears.rxrepo.test.Product;
import com.slimgears.rxrepo.test.UniqueId;
import com.slimgears.rxrepo.util.Expressions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.slimgears.rxrepo.test.UniqueId.inventoryId;
import static com.slimgears.rxrepo.test.UniqueId.productId;

public class ExpressionsTest {
    private final Product product1 = Product.builder()
            .key(productId(0))
                .name("Product 0")
                .price(9)
                .inventory(Inventory.builder().name("Inventory 1").id(inventoryId(1)).build())
            .build();

    private final Product product2 = Product.builder()
            .key(productId(10))
                .name("Product 10")
                .price(9)
                .inventory(Inventory.builder().name("Inventory 1").id(inventoryId(1)).build())
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

    @Test
    public void testSearchTextExpression() {
        Predicate<Product> predicate = Expressions.compilePredicate(Product.$.searchText("Product 1 - ["));
        Assert.assertTrue(predicate.test(Product.builder()
                .key(UniqueId.productId(1))
                .price(1)
                .name("Product 1 - [ test ]")
                .build()));
    }

    @Test
    public void testExpressionToString() {
        Assert.assertEquals("Equals(Length(Argument(Product).name), Argument(Product).price)", Product.$.name.length().eq(Product.$.price).toString());
    }
}
