package pl.mwisniewski.statistics.domain.port;

import pl.mwisniewski.statistics.domain.model.AggregatesQuery;
import pl.mwisniewski.statistics.domain.model.AggregatesQueryResult;

public interface StatisticsRepository {
    AggregatesQueryResult getAggregates(AggregatesQuery query);
}
