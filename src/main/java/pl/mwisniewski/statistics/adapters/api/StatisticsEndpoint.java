package pl.mwisniewski.statistics.adapters.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import pl.mwisniewski.statistics.domain.StatisticsService;
import pl.mwisniewski.statistics.domain.model.*;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
        AggregatesQuery query = domainQuery(timeRangeStr, action, aggregates, origin, brandId, categoryId);
        AggregatesQueryResult result = statisticsService.getAggregates(query);
        AggregatesResponse response = createEndpointResponse(query, result);

        return ResponseEntity.ok(Objects.requireNonNullElse(expectedResult, response));
    }

    private AggregatesQuery domainQuery(String timeRangeStr, Action action, List<Aggregate> aggregates,
                                        String origin, String brandId, String categoryId) {
        return new AggregatesQuery(
                domainTimeRange(timeRangeStr),
                action,
                aggregates,
                Optional.ofNullable(origin),
                Optional.ofNullable(brandId),
                Optional.ofNullable(categoryId)
        );
    }

    private TimeRange domainTimeRange(String timeRangeStr) {
        String[] splitTimeRange = timeRangeStr.split("_");
        String startTimeRange = splitTimeRange[0] + DEFAULT_TIMEZONE_SUFFIX;
        String endTimeRange = splitTimeRange[1] + DEFAULT_TIMEZONE_SUFFIX;

        return new TimeRange(
                ZonedDateTime.parse(startTimeRange).toInstant().toEpochMilli(),
                ZonedDateTime.parse(endTimeRange).toInstant().toEpochMilli()
        );
    }

    private AggregatesResponse createEndpointResponse(AggregatesQuery query,
                                                      AggregatesQueryResult result) {
        return AggregatesResponse.of(query, result);
    }

    private static final String DEFAULT_TIMEZONE_SUFFIX = "Z";
}
