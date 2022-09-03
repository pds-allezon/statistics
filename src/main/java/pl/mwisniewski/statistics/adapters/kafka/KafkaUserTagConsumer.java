package pl.mwisniewski.statistics.adapters.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.mwisniewski.statistics.adapters.UserTagEvent;
import pl.mwisniewski.statistics.adapters.mongo.MongoStatisticsRepository;

@Component
@Profile("prod")
public class KafkaUserTagConsumer {
    final MongoStatisticsRepository mongoStatisticsRepository;

    public KafkaUserTagConsumer(MongoStatisticsRepository mongoStatisticsRepository) {
        this.mongoStatisticsRepository = mongoStatisticsRepository;
    }

    @KafkaListener(topics = "user-tags", groupId = "statistics")
    void consume(UserTagEvent userTagEvent) {
        logger.debug("Consuming user tag event: {}", userTagEvent);
        mongoStatisticsRepository.addUserTagStats(userTagEvent);
    }

    private final Logger logger = LoggerFactory.getLogger(KafkaUserTagConsumer.class);
}
