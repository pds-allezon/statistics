package pl.mwisniewski.statistics.adapters.dummy;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import pl.mwisniewski.statistics.domain.model.AggregatesQuery;
import pl.mwisniewski.statistics.domain.model.AggregatesQueryResult;
import pl.mwisniewski.statistics.domain.port.StatisticsRepository;

import java.util.List;

@Component
@Profile("test")
public class DummyStatisticsRepository implements StatisticsRepository {

    @Override
    public AggregatesQueryResult getAggregates(AggregatesQuery query) {
        return new AggregatesQueryResult(List.of());
    }
}
