package com.slimgears.rxrepo.mongodb;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.expressions.*;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.util.PropertyReference;
import com.slimgears.rxrepo.util.PropertyReferences;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.reflect.TypeToken;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MongoQueries {
    private final static Logger log = LoggerFactory.getLogger(MongoQueries.class);

    public static <K, S extends HasMetaClassWithKey<K, S>> Document filterFor(S entity) {
        return MongoQueries.filterForKey(HasMetaClassWithKey.keyOf(entity));
    }

    static <K> Document filterForKey(K key) {
        return new Document("_id", key);
    }

    static <K> Document filterForKeyAndVersion(K key, long version) {
        return filterForKey(key).append("_version", version);
    }

    static List<Document> aggregationPipeline(QueryInfo<?, ?, ?> queryInfo) {
        return aggregationPipeline(queryInfo, null);
    }

    private static <K, S extends HasMetaClassWithKey<K, S>, T> List<Document> aggregationPipeline(QueryInfo<K, S, T> queryInfo, Aggregator<T, T, ?> aggregator) {
        ImmutableList.Builder<Document> builder = ImmutableList.builder();
        builder.addAll(MongoQueries.lookupAndUnwindReferences(queryInfo.metaClass()));

        Optional.ofNullable(queryInfo.predicate())
                .map(MongoQueries::match)
                .ifPresent(builder::add);

        Optional.of(queryInfo.sorting())
                .filter(si -> !si.isEmpty())
                .map(MongoQueries::toSorting)
                .map(sorting -> new Document("$sort", sorting))
                .ifPresent(builder::add);

        Optional.ofNullable(queryInfo.skip())
                .map(skip -> new Document("$skip", skip))
                .ifPresent(builder::add);

        Optional.ofNullable(queryInfo.limit())
                .map(limit -> new Document("$limit", limit))
                .ifPresent(builder::add);

        Optional.ofNullable(queryInfo.mapping())
                .filter(exp -> !(exp instanceof ArgumentExpression))
                .map(MongoQueries::expr)
                .map(expr -> new Document("$replaceWith", expr))
                .ifPresent(builder::add);

        Optional.of(queryInfo.properties())
                .filter(p -> !p.isEmpty())
                .map(MongoQueries::toProjection)
                .map(projection -> new Document("$project", projection))
                .ifPresent(builder::add);

        Optional.ofNullable(aggregator)
                .map(ag -> aggregation(queryInfo.objectType(), ag))
                .ifPresent(builder::add);

        List<Document> pipeline = builder.build();
        pipeline.forEach(d -> log.debug("Pipeline element: {}", d));
        return pipeline;
    }

    public static Document expr(ObjectExpression<?, ?> expression) {
        if (expression == null) {
            return new Document();
        }

        Object obj = new MongoExpressionAdapter().visit(expression, null);
        return new Document("$expr", obj);
    }

    private static Document match(ObjectExpression<?, ?> expression) {
        return new Document("$match", expr(expression));
    }

    public static <T> Document aggregation(TypeToken<T> type, Aggregator<T, T, ?> aggregator) {
        UnaryOperationExpression<T, Collection<T>, ?> expression = aggregator.apply(CollectionExpression.indirectArg(type));
        return (Document)new MongoExpressionAdapter().visit(expression, null);
    }

    public static List<Document> lookupAndUnwindReferences(MetaClass<?> metaClass) {
        return PropertyReferences.forMetaClass(metaClass)
                .stream()
                .flatMap(pr -> Stream.of(lookup(pr), unwind(pr)))
                .collect(Collectors.toList());
    }

    private static Document lookup(PropertyReference propertyReference) {
        return lookup(propertyReference.referencePath(), propertyReference.property());
    }

    private static Document unwind(PropertyReference propertyReference) {
        return unwind(propertyReference.referencePath(), propertyReference.property());
    }

    private static Document lookup(String prefixPath, PropertyMeta<?, ?> propertyMeta) {
        MetaClassWithKey<?, ?> targetMeta = MetaClasses.forTokenWithKeyUnchecked(propertyMeta.type());
        if (targetMeta == null) {
            throw new RuntimeException(MoreStrings.format("Property {}.{} is not reference property", propertyMeta.declaringType().simpleName(), propertyMeta.name()));
        }
        return new Document("$lookup",
                new Document()
                        .append("from", targetMeta.simpleName())
                        .append("localField", prefixPath + propertyMeta.name())
                        .append("foreignField", "_id")
                        .append("as", prefixPath + propertyMeta.name()));
    }

    private static Document unwind(String prefixPath, PropertyMeta<?, ?> propertyMeta) {
        return new Document("$unwind",
                new Document()
                .append("path", "$" + prefixPath + propertyMeta.name())
                .append("preserveNullAndEmptyArrays", true));
    }

    private static <T> Document toProjection(Iterable<PropertyExpression<T, ?, ?>> properties) {
        Document projection = new Document();
        properties.forEach(p -> projection.append(propertyToString(p), 1));
        return projection;
    }

    private static <T> Document toSorting(Iterable<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos) {
        Document sorting = new Document();
        sortingInfos.forEach(si -> sorting.append(propertyToString(si.property()), si.ascending() ? 1 : -1));
        return sorting;
    }

    private static String propertyToString(PropertyExpression<?, ?, ?> property) {
        if (property.target().type().operationType() == Expression.OperationType.Property) {
            return propertyToString((PropertyExpression<?, ?, ?>)property.target()) + "." + property.property().name();
        }
        return property.property().name();
    }
}
