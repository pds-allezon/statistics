package pl.mwisniewski.statistics.adapters.mongo;

import java.math.BigInteger;

public record AggregatesResultDocument(String bucket,
                                       BigInteger sumPrice,
                                       BigInteger count) {
}
