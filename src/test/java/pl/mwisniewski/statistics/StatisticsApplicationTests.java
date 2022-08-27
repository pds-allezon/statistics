package pl.mwisniewski.statistics;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import pl.mwisniewski.statistics.adapters.api.AggregatesResponse;
import pl.mwisniewski.statistics.adapters.api.StatisticsEndpoint;
import pl.mwisniewski.statistics.domain.model.Action;
import pl.mwisniewski.statistics.domain.model.Aggregate;

import java.util.List;

@SpringBootTest
@ActiveProfiles("test")
class StatisticsApplicationTests {

    @Autowired
    private StatisticsEndpoint endpoint;

    @Test
    void contextLoads() {
    }

    @Test
    void shouldReturnExpectedResultIfPresent() {
        // given
        String timeRange = "2022-03-22T12:15:00.000_2022-03-22T12:30:00.000";
        Action action = Action.BUY;
        List<Aggregate> aggregates = List.of(Aggregate.COUNT);

        List<String> expectedColumns = List.of("1m_bucket", "action", "sum_price");
        List<List<String>> expectedRows = List.of(
                List.of("bucket1", "BUY", "123"),
                List.of("bucket2", "BUY", "321")
        );

        AggregatesResponse expectedResult = new AggregatesResponse(
                expectedColumns, expectedRows
        );

        // when
        ResponseEntity<AggregatesResponse> response = endpoint.aggregates(
                timeRange, action, aggregates, null, null, null, expectedResult
        );

        // then
        assert response.getBody() == expectedResult;
    }

}
