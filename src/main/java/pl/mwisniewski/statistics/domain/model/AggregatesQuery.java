package pl.mwisniewski.statistics.domain.model;

import java.util.List;
import java.util.Optional;

public record AggregatesQuery(
        TimeRange timeRange,
        Action action,
        List<Aggregate> aggregates,
        Optional<String> origin,
        Optional<String> brandId,
        Optional<String> categoryId
) {
}
