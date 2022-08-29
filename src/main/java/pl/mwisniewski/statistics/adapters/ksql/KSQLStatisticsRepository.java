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

import java.util.Comparator;
import java.util.List;

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
                resultRows.stream()
                        .map(it -> toDomainRow(query, it))
                        .sorted(Comparator.comparing(QueryResultRow::timeBucketStr))
                        .toList()
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

    private QueryResultRow toDomainRow(AggregatesQuery query, Row ksqlRow) {
        return new QueryResultRow(
                ksqlRow.getString("BUCKET") + ":00",
                Action.valueOf(ksqlRow.getString("ACTION")),
                query.origin().map(it -> ksqlRow.getString("ORIGIN")),
                query.brandId().map(it -> ksqlRow.getString("BRANDID")),
                query.categoryId().map(it -> ksqlRow.getString("CATEGORYID")),
                ksqlRow.getLong("SUMPRICE"),
                ksqlRow.getLong("COUNT")
        );
    }
}
