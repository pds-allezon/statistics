package pl.mwisniewski.statistics.adapters.mongo;

import com.mongodb.client.model.*;
import org.bson.conversions.Bson;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;
import pl.mwisniewski.statistics.domain.model.AggregatesQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Repository
@Profile("prod")
class MongoPersistentRepository {
    final MongoTemplate mongoTemplate;

    public MongoPersistentRepository(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    void addDocument(StatisticsEntryDocument document) {
        Bson filter = Filters.and(
                Filters.eq("bucket", document.bucket()),
                Filters.eq("action", document.action()),
                Filters.eq("origin", document.origin()),
                Filters.eq("brandId", document.brandId()),
                Filters.eq("categoryId", document.categoryId())
        );
        Bson update = Updates.combine(
                Updates.inc("sumPrice", document.sumPrice()),
                Updates.inc("count", document.count()),
                Updates.setOnInsert("bucket", document.bucket()),
                Updates.setOnInsert("action", document.action()),
                Updates.setOnInsert("origin", document.origin()),
                Updates.setOnInsert("brandId", document.brandId()),
                Updates.setOnInsert("categoryId", document.categoryId())
        );

        mongoTemplate
                .getCollection(STATISTICS_COLLECTION_NAME)
                .updateOne(filter, update, new UpdateOptions().upsert(true));
    }

    List<AggregatesResultDocument> retrieveDocuments(AggregatesQuery query) {
        Bson aggregateMatch = Aggregates.match(Filters.and(generateAggregateFilters(query)));
        Bson aggregateGroup = Aggregates.group(
                "$bucket",
                Accumulators.sum("sumPrice", "$sumPrice"),
                Accumulators.sum("count", "$count")
        );

        List<AggregatesResultDocument> result = new ArrayList<>();
        mongoTemplate
                .getCollection(STATISTICS_COLLECTION_NAME)
                .aggregate(List.of(aggregateMatch, aggregateGroup))
                .map(doc ->
                        new AggregatesResultDocument(
                                doc.getString("_id") + ":00",
                                doc.getLong("sumPrice"),
                                doc.getLong("count")
                        )
                ).forEach(result::add);

        return result;
    }

    private List<Bson> generateAggregateFilters(AggregatesQuery query) {
        return Stream.of(
                        Filters.gte("bucket", query.bucketRange().startBucket()),
                        Filters.lt("bucket", query.bucketRange().endBucket()),
                        Filters.eq("action", query.action()),
                        query.origin().map(it -> Filters.eq("origin", it)).orElse(null),
                        query.brandId().map(it -> Filters.eq("brandId", it)).orElse(null),
                        query.categoryId().map(it -> Filters.eq("categoryId", it)).orElse(null)
                )
                .filter(Objects::nonNull)
                .toList();
    }

    private static final String STATISTICS_COLLECTION_NAME = "statistics";
}
