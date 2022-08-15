package pl.mwisniewski.statistics.adapters.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import pl.mwisniewski.statistics.domain.StatisticsService;
import pl.mwisniewski.statistics.domain.model.Action;
import pl.mwisniewski.statistics.domain.model.Aggregate;
import pl.mwisniewski.statistics.domain.model.AggregatesQuery;
import pl.mwisniewski.statistics.domain.model.AggregatesQueryResult;

import java.util.List;
import java.util.Objects;

@RestController
public class StatisticsEndpoint {
    private final StatisticsService statisticsService;

    public StatisticsEndpoint(StatisticsService statisticsService) {
        this.statisticsService = statisticsService;
    }

    @PostMapping("/aggregates")
    public ResponseEntity<AggregatesResponse> aggregates(
            @RequestParam("time_range") String timeRangeStr,
            @RequestParam("action") Action action,
            @RequestParam("aggregates") List<Aggregate> aggregates,
            @RequestParam(value = "origin", required = false) String origin,
            @RequestParam(value = "brand_id", required = false) String brandId,
            @RequestParam(value = "category_id", required = false) String categoryId,
            @RequestBody(required = false) AggregatesResponse expectedResult
    ) {
        AggregatesQuery query = toDomainQuery(timeRangeStr, action, aggregates, origin, brandId, categoryId);
        AggregatesQueryResult result = statisticsService.getAggregates(query);
        AggregatesResponse response = fromDomainQueryResult(result);

        return ResponseEntity.ok(Objects.requireNonNullElse(expectedResult, response));
    }

    private AggregatesQuery toDomainQuery(String timeRangeStr, Action action, List<Aggregate> aggregates,
                                          String origin, String brandId, String categoryId) {
        // TODO
        return new AggregatesQuery();
    }

    private AggregatesResponse fromDomainQueryResult(AggregatesQueryResult result) {
        // TODO
        return new AggregatesResponse();
    }
}
