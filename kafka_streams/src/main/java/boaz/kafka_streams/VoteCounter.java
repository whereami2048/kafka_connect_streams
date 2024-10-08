package boaz.kafka_streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class VoteCounter {

	private static final ObjectMapper mapper = new ObjectMapper();

	public static void main(String[] args) {
		Properties props = KafkaStreamProperty.of();

		//스트림 토폴로지 정의를 위한 객체 생성
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		//소스 프로세서 동작
		KStream<String, String> stream = streamsBuilder.stream(KafkaStreamProperty.RECEIVE_TOPIC_NAME);

		//스트림 프로세서 동작
		KTable<String, String> countTable = stream
				.groupByKey()
				.count()
				.mapValues((key, value) -> {
					HashMap<String, String> countMap = new HashMap<>();
					countMap.put("vote_num", key);
					countMap.put("count", value.toString());

					try {
						return mapper.writeValueAsString(countMap);
					} catch (JsonProcessingException e) {
						throw new RuntimeException(e);
					}
				});

		//싱크 프로세서 동작
		countTable.toStream().to(KafkaStreamProperty.SEND_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.String()));

		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
}
