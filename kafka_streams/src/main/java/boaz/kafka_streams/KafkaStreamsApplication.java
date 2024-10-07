package boaz.kafka_streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.rmi.ServerError;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class KafkaStreamsApplication {

	private static final String KAFKA_LOCAL_BOOTSTRAP_SERVER = "localhost:9092";
	private static final String KAFKA_BOOTSTRAP_SERVER = "43.202.21.236:9092";
	private static final String RECEIVE_TOPIC_NAME = "message-topic";
	private static final String SEND_TOPIC_NAME = "result-topic";
	private static final ObjectMapper mapper = new ObjectMapper();

	public static void main(String[] args) {
		Properties props = new Properties();
		// 카프카 스트림즈를 유일하게 구분할 ID값
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-application");

		// 스트림즈에 접근할 카프카 broker 정보
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);

		// 데이터를 어떤 형식으로 Read/Write 할지 성정 (키/값의 데이터 타입을 지정)
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		//스트림 토폴로지 정의를 위한 객체 생성
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		//소스 프로세서 동작
		KStream<String, String> stream = streamsBuilder.stream(RECEIVE_TOPIC_NAME);

		//스트림 프로세서 동작
		KTable<String, String> countTable = stream
				.groupByKey()
				.count()
						.mapValues(value -> {
							Count count = new Count(value);
                            try {
                                return mapper.writeValueAsString(count);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        });

		//싱크 프로세서 동작
		countTable.toStream().to(SEND_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.String()));

		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
		kafkaStreams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}
}
