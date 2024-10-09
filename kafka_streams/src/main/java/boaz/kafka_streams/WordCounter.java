package boaz.kafka_streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class WordCounter {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = KafkaStreamProperty.of("text2-application");

        //스트림 토폴로지 정의를 위한 객체 생성
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //소스 프로세서 동작
        KStream<String, String> stream = streamsBuilder.stream(KafkaStreamProperty.RECEIVE_TEXT_TOPIC_NAME);

        //스트림 프로세서 동작
        KTable<String, Long> wordCounts = stream
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .groupBy((key, word) -> word)
                .count(Materialized.with(Serdes.String(), Serdes.Long()));

        KStream<String, String> resultStream = wordCounts
                .toStream()
                .mapValues((word, count) -> {
                    HashMap<String, Long> countMap = new HashMap<>();
                    countMap.put(word, count);

                    try {
                        return mapper.writeValueAsString(countMap);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });

        //싱크 프로세서 동작
        resultStream.to(KafkaStreamProperty.SEND_TEXT_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
