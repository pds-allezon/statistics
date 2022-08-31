package pl.mwisniewski.statistics.adapters.ksql;

import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import pl.mwisniewski.statistics.domain.model.Action;
import pl.mwisniewski.statistics.domain.model.AggregatesQuery;
import pl.mwisniewski.statistics.domain.model.AggregatesQueryResult;
import pl.mwisniewski.statistics.domain.model.QueryResultRow;
import pl.mwisniewski.statistics.domain.port.StatisticsRepository;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Profile("prod")
public class KSQLStatisticsRepository implements StatisticsRepository {
    private final Client client;

    public KSQLStatisticsRepository(
            @Value("${aggregates.ksql.server.host}") String host,
            @Value("${aggregates.ksql.server.port}") int port
    ) {
        ClientOptions options = ClientOptions.create()
                .setHost(host)
                .setPort(port);

        this.client = Client.create(options);
    }

    @Override
    public AggregatesQueryResult getAggregates(AggregatesQuery query) {
        String ksqlQuery = aggregatesKSQLQuery(query);
        BatchedQueryResult batchedQueryResult = client.executeQuery(ksqlQuery);

        List<Row> resultRows;
        try {
            resultRows = batchedQueryResult.get();
        } catch (Exception e) {
            throw new RuntimeException("Could not get rows from ksql database", e);
        }

        return new AggregatesQueryResult(
                generateResultRows(query, resultRows)
        );
    }

    private String aggregatesKSQLQuery(AggregatesQuery query) {
        KSQLQueryBuilder builder = KSQLQueryBuilder.builder(
                query.bucketRange().startBucket(), query.bucketRange().endBucket(), query.action().toString()
        );

        builder = query.origin().map(builder::withOrigin).orElse(builder);
        builder = query.brandId().map(builder::withBrandId).orElse(builder);
        builder = query.categoryId().map(builder::withCategoryId).orElse(builder);

        return builder.build();
    }

    private List<QueryResultRow> generateResultRows(AggregatesQuery query, List<Row> ksqlRows) {
        List<String> buckets = generateBuckets(
                query.bucketRange().startBucket(), query.bucketRange().endBucket()
        );

        List<QueryResultRow> domainRows = ksqlRows.stream().map(it -> toDomainRow(query, it)).toList();

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
                                rowsPerBucket.containsKey(bucket) ? sumPrice(rowsPerBucket.get(bucket)) : BigInteger.ZERO,
                                rowsPerBucket.containsKey(bucket) ? sumCount(rowsPerBucket.get(bucket)) : BigInteger.ZERO
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

    private QueryResultRow toDomainRow(AggregatesQuery query, Row ksqlRow) {
        return new QueryResultRow(
                ksqlRow.getString("BUCKET") + ":00",
                Action.valueOf(ksqlRow.getString("ACTION")),
                query.origin().map(it -> ksqlRow.getString("ORIGIN")),
                query.brandId().map(it -> ksqlRow.getString("BRANDID")),
                query.categoryId().map(it -> ksqlRow.getString("CATEGORYID")),
                BigInteger.valueOf(ksqlRow.getLong("SUMPRICE")),
                BigInteger.valueOf(ksqlRow.getLong("COUNT"))
        );
    }

    private BigInteger sumPrice(List<QueryResultRow> rows) {
        return rows.stream().map(QueryResultRow::sumPrice).reduce(BigInteger::add).get();
    }

    private BigInteger sumCount(List<QueryResultRow> rows) {
        return rows.stream().map(QueryResultRow::count).reduce(BigInteger::add).get();
    }
}
