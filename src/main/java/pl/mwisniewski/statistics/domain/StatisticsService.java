package pl.mwisniewski.statistics.domain;

import org.springframework.stereotype.Component;
import pl.mwisniewski.statistics.domain.model.AggregatesQuery;
import pl.mwisniewski.statistics.domain.model.AggregatesQueryResult;
import pl.mwisniewski.statistics.domain.port.StatisticsRepository;

@Component
public class StatisticsService {
    private final StatisticsRepository statisticsProvider;

    public StatisticsService(StatisticsRepository statisticsRepository) {
        this.statisticsProvider = statisticsRepository;
    }

    public AggregatesQueryResult getAggregates(AggregatesQuery query) {
        return statisticsProvider.getAggregates(query);
    }
}
