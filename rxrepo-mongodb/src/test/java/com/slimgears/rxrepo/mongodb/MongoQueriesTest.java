package com.slimgears.rxrepo.mongodb;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.test.Product;
import com.slimgears.rxrepo.test.ProductDescription;
import com.slimgears.util.reflect.TypeToken;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;

public class MongoQueriesTest {
    @Test
    public void testPropertyExpression() {
        Document filter = MongoQueries.expr(Product.$.inventory.name.eq("Product 2"));
        Assert.assertEquals("{\"$expr\": {\"$eq\": [\"$inventory.name\", \"Product 2\"]}}", filter.toJson());
    }

    @Test
    public void testBooleanAndExpression() {
        Document filter = MongoQueries.expr(
                Product.$.inventory.name.eq("Product 2")
                .and(Product.$.price.betweenInclusive(101, 106)));
        Assert.assertEquals("{\"$expr\": {\"$and\": [{\"$eq\": [\"$inventory.name\", \"Product 2\"]}, {\"$not\": {\"$or\": [{\"$lt\": [\"$price\", 101]}, {\"$gt\": [\"$price\", 106]}]}}]}}", filter.toJson());
    }

    @Test
    public void testBooleanOrExpression() {
        Document filter = MongoQueries.expr(
                Product.$.inventory.name.eq("Product 2")
                .or(Product.$.price.lessOrEqual(101)));
        Assert.assertEquals("{\"$expr\": {\"$or\": [{\"$eq\": [\"$inventory.name\", \"Product 2\"]}, {\"$not\": {\"$gt\": [\"$price\", 101]}}]}}", filter.toJson());
    }

    @Test
    public void testLookupFromMetaClass() {
        String json = MongoQueries
                .lookupAndUnwindReferences(ProductDescription.metaClass)
                .stream()
                .map(Document::toJson)
                .collect(Collectors.joining("\n"));
        Assert.assertEquals(
                "{\"$lookup\": {\"from\": \"Product\", \"localField\": \"product\", \"foreignField\": \"_id\", \"as\": \"product\"}}\n" +
                        "{\"$unwind\": {\"path\": \"$product\", \"preserveNullAndEmptyArrays\": true}}\n" +
                        "{\"$lookup\": {\"from\": \"Inventory\", \"localField\": \"product.inventory\", \"foreignField\": \"_id\", \"as\": \"product.inventory\"}}\n" +
                        "{\"$unwind\": {\"path\": \"$product.inventory\", \"preserveNullAndEmptyArrays\": true}}\n" +
                        "{\"$lookup\": {\"from\": \"Inventory\", \"localField\": \"product.inventory.inventory\", \"foreignField\": \"_id\", \"as\": \"product.inventory.inventory\"}}\n" +
                        "{\"$unwind\": {\"path\": \"$product.inventory.inventory\", \"preserveNullAndEmptyArrays\": true}}",
                json);
    }

    @Test
    public void testAggregateCount() {
        Document doc = MongoQueries.aggregation(TypeToken.of(Product.class), Aggregator.count());
        Assert.assertEquals("{\"$count\": \"__aggregation\"}", doc.toJson());
    }
}
