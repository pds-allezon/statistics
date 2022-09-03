package pl.mwisniewski.statistics.adapters.mongo;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigInteger;

@Document("statistics")
public record StatisticsEntryDocument(@Id String bucket,
                                      String action,
                                      String origin,
                                      String brandId,
                                      String categoryId,
                                      BigInteger sumPrice,
                                      BigInteger count) {
}
