package pl.mwisniewski.statistics.adapters.mongo;

public record AggregatesResultDocument(String bucket,
                                       long sumPrice,
                                       long count) {
}
