package boaz.kafka_streams;


public record Count(
    Long count
) {
    public Count(Long count) {
        this.count = count;
    }
}
