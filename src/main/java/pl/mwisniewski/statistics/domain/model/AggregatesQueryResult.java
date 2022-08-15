package pl.mwisniewski.statistics.domain.model;

import java.util.List;

public record AggregatesQueryResult(
        List<QueryResultRow> rows
) {
}
