package pl.mwisniewski.statistics.adapters.api;

import pl.mwisniewski.statistics.domain.model.Aggregate;
import pl.mwisniewski.statistics.domain.model.AggregatesQuery;
import pl.mwisniewski.statistics.domain.model.AggregatesQueryResult;
import pl.mwisniewski.statistics.domain.model.QueryResultRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public record AggregatesResponse(
        List<String> columns,
        List<List<String>> rows
) {
    public static AggregatesResponse of(AggregatesQuery query,
                                        AggregatesQueryResult result) {
        return new AggregatesResponse(
                createResultColumns(query),
                createResultRows(result.rows(), query.aggregates())
        );
    }

    private static List<String> createResultColumns(AggregatesQuery query) {
        List<String> columns = new ArrayList<>(Stream.of(
                BUCKET_COLUMN_NAME,
                ACTION_COLUMN_NAME,
                query.origin().isPresent() ? ORIGIN_COLUMN_NAME : null,
                query.brandId().isPresent() ? BRAND_ID_COLUMN_NAME : null,
                query.categoryId().isPresent() ? CATEGORY_ID_COLUMN_NAME : null
        ).filter(Objects::nonNull).toList());
        columns.addAll(createAggregatesColumns(query.aggregates()));

        return columns;
    }

    private static List<String> createAggregatesColumns(List<Aggregate> aggregates) {
        return aggregates.stream()
                .map(it -> it == Aggregate.COUNT ? COUNT_COLUMN_NAME : SUM_PRICE_COLUMN_NAME)
                .toList();
    }

    private static List<List<String>> createResultRows(List<QueryResultRow> rows, List<Aggregate> aggregates) {
        return rows.stream()
                .map(it -> createResultRow(it, aggregates))
                .toList();
    }

    private static List<String> createResultRow(QueryResultRow row, List<Aggregate> aggregates) {
        List<String> values = new ArrayList<>(Stream.of(
                row.timeBucketStr(),
                row.action().toString(),
                row.origin().orElse(null),
                row.brandId().orElse(null),
                row.categoryId().orElse(null)
        ).filter(Objects::nonNull).toList());
        values.addAll(createAggregatesRow(row, aggregates));

        return values;
    }

    private static List<String> createAggregatesRow(QueryResultRow row, List<Aggregate> aggregates) {
        List<Long> values = new ArrayList<>();
        values.add(aggregates.get(0) == Aggregate.COUNT ? row.count() : row.sumPrice());

        if (aggregates.size() > 1) {
            values.add(aggregates.get(1) == Aggregate.COUNT ? row.count() : row.sumPrice());
        }

        return values.stream()
                .map(Object::toString)
                .toList();
    }

    private static final String BUCKET_COLUMN_NAME = "1m_bucket";
    private static final String ACTION_COLUMN_NAME = "action";
    private static final String ORIGIN_COLUMN_NAME = "origin";
    private static final String BRAND_ID_COLUMN_NAME = "brand_id";
    private static final String CATEGORY_ID_COLUMN_NAME = "category_id";
    private static final String COUNT_COLUMN_NAME = "count";
    private static final String SUM_PRICE_COLUMN_NAME = "sum_price";
}
