package boaz.kafka_streams;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class WordCounter {

    public static void main(String[] args) {
        Properties props = KafkaStreamProperty.of();

        //스트림 토폴로지 정의를 위한 객체 생성
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //소스 프로세서 동작
        KStream<String, String> stream = streamsBuilder.stream(KafkaStreamProperty.RECEIVE_TOPIC_NAME);

        //스트림 프로세서 동작
        stream.filter((key, value) -> "others".equals(key));

        KTable<String, Long> wordCounts = stream
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        KStream<String, String> resultStream = wordCounts
                .toStream()
                .mapValues((word, count) -> word + " : " + count);

        //싱크 프로세서 동작
        resultStream.to(KafkaStreamProperty.SEND_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
