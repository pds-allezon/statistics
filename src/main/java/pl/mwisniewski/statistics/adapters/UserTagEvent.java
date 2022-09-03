package pl.mwisniewski.statistics.adapters;

public record UserTagEvent(
        String time,
        String cookie,
        String country,
        String device,
        String action,
        String origin,
        ProductInfoEvent productInfo
) {
}
