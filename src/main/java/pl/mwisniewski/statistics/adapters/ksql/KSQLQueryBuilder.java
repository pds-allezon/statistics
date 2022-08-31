package pl.mwisniewski.statistics.adapters.ksql;

import java.util.Optional;

public class KSQLQueryBuilder {
    private final String startBucket;
    private final String endBucket;
    private final String action;
    private Optional<String> origin = Optional.empty();
    private Optional<String> brandId = Optional.empty();
    private Optional<String> categoryId = Optional.empty();

    private KSQLQueryBuilder(String startBucket, String endBucket, String action) {
        this.startBucket = startBucket;
        this.endBucket = endBucket;
        this.action = action;
    }

    public static KSQLQueryBuilder builder(String startBucket, String endBucket, String action) {
        return new KSQLQueryBuilder(startBucket, endBucket, action);
    }

    public String build() {
        return "%s %s %s;".formatted(select(), from(), where());
    }

    private String select() {
        return "SELECT *";
    }

    private String from() {
        return "FROM aggregates";
    }

    private String where() {
        String where = "WHERE bucket >= '%s' AND bucket < '%s' AND action = '%s'"
                .formatted(this.startBucket, this.endBucket, this.action);
        where += origin.isPresent() ? " AND origin = '%s'".formatted(origin.get()) : "";
        where += brandId.isPresent() ? " AND brandId = '%s'".formatted(brandId.get()) : "";
        where += categoryId.isPresent() ? " AND categoryId = '%s'".formatted(categoryId.get()) : "";

        return where;
    }

    public KSQLQueryBuilder withOrigin(String origin) {
        this.origin = Optional.of(origin);
        return this;
    }

    public KSQLQueryBuilder withBrandId(String brandId) {
        this.brandId = Optional.of(brandId);
        return this;
    }

    public KSQLQueryBuilder withCategoryId(String categoryId) {
        this.categoryId = Optional.of(categoryId);
        return this;
    }
}
