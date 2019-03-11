package com.slimgears.rxorm.orientdb;

import com.slimgears.util.repository.expressions.ObjectExpression;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionGeneratorTest {
    @Test
    public void testExpression() {
        ObjectExpression<Product, Boolean> expression = Product.$.name.eq("product1")
                .and(Product.$.id.greaterThan(5L))
                .or(Product.$.inventory.name.eq("inventory1"));


        String exp = SqlExpressionGenerator.toSqlExpression(expression);
        Assert.assertEquals("(((`name` = 'product1') and (`id` > 5)) or (`inventory`.`name` = 'inventory1'))", exp);
    }
}
