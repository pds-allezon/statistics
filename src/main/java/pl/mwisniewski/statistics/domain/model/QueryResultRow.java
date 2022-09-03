package pl.mwisniewski.statistics.domain.model;

import java.util.Optional;

public record QueryResultRow(
        String timeBucketStr,
        Action action,
        Optional<String> origin,
        Optional<String> brandId,
        Optional<String> categoryId,
        long sumPrice,
        long count
) {
}
