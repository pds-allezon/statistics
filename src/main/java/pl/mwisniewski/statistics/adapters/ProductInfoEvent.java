package pl.mwisniewski.statistics.adapters;

public record ProductInfoEvent(
        String productId,
        String brandId,
        String categoryId,
        int price
) {
}
