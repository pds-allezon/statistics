package pl.mwisniewski.statistics.adapters.mongo;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import pl.mwisniewski.statistics.adapters.UserTagEvent;
import pl.mwisniewski.statistics.domain.model.AggregatesQuery;
import pl.mwisniewski.statistics.domain.model.AggregatesQueryResult;
import pl.mwisniewski.statistics.domain.model.QueryResultRow;
import pl.mwisniewski.statistics.domain.port.StatisticsRepository;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Profile("prod")
public class MongoStatisticsRepository implements StatisticsRepository {
    private final MongoPersistentRepository mongoPersistentRepository;

    public MongoStatisticsRepository(MongoPersistentRepository mongoPersistentRepository) {
        this.mongoPersistentRepository = mongoPersistentRepository;
    }

    public void addUserTagStats(UserTagEvent userTagEvent) {
        StatisticsEntryDocument document = new StatisticsEntryDocument(
                timeToBucket(userTagEvent.time()),
                userTagEvent.action(),
                userTagEvent.origin(),
                userTagEvent.productInfo().brandId(),
                userTagEvent.productInfo().categoryId(),
                userTagEvent.productInfo().price(),
                1
        );
        mongoPersistentRepository.addDocument(document);
    }

    @Override
    public AggregatesQueryResult getAggregates(AggregatesQuery query) {
        List<AggregatesResultDocument> documents = mongoPersistentRepository.retrieveDocuments(query);

        return new AggregatesQueryResult(
                generateResultRows(query, documents)
        );
    }

    private List<QueryResultRow> generateResultRows(AggregatesQuery query, List<AggregatesResultDocument> documents) {
        List<String> buckets = generateBuckets(
                query.bucketRange().startBucket(), query.bucketRange().endBucket()
        );
        List<QueryResultRow> domainRows = documents.stream().map(it -> toDomainRow(query, it)).toList();

        Map<String, List<QueryResultRow>> rowsPerBucket = domainRows.stream()
                .collect(Collectors.groupingBy(QueryResultRow::timeBucketStr));

        return buckets
                .stream()
                .map(bucket -> new QueryResultRow(
                                bucket,
                                query.action(),
                                query.origin(),
                                query.brandId(),
                                query.categoryId(),
                                rowsPerBucket.containsKey(bucket) ? sumPrice(rowsPerBucket.get(bucket)) : 0L,
                                rowsPerBucket.containsKey(bucket) ? sumCount(rowsPerBucket.get(bucket)) : 0L
                        )
                )
                .sorted(Comparator.comparing(QueryResultRow::timeBucketStr))
                .toList();
    }

    private List<String> generateBuckets(String startBucketInclusive, String endBucketExclusive) {
        LocalDateTime startBucket = LocalDateTime.parse(startBucketInclusive);
        LocalDateTime endBucket = LocalDateTime.parse(endBucketExclusive);

        List<String> buckets = new ArrayList<>();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        do {
            buckets.add(startBucket.format(formatter));
            startBucket = startBucket.plusMinutes(1);
        } while (startBucket.compareTo(endBucket) < 0);

        return buckets;
    }

    private QueryResultRow toDomainRow(AggregatesQuery query, AggregatesResultDocument document) {
        return new QueryResultRow(
                document.bucket(),
                query.action(),
                query.origin(),
                query.brandId(),
                query.categoryId(),
                document.sumPrice(),
                document.count()
        );
    }

    private long sumPrice(List<QueryResultRow> rows) {
        return rows.stream().map(QueryResultRow::sumPrice).reduce(Long::sum).get();
    }

    private long sumCount(List<QueryResultRow> rows) {
        return rows.stream().map(QueryResultRow::count).reduce(Long::sum).get();
    }

    private String timeToBucket(String time) {
        return time.substring(0, 16);
    }
}
