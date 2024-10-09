package boaz.kafka_streams;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class KafkaStreamProperty {

    static final String KAFKA_LOCAL_BOOTSTRAP_SERVER = "localhost:9092";
    static final String KAFKA_BOOTSTRAP_SERVER = "";

    static final String RECEIVE_VOTE_TOPIC_NAME = "vote-source-topic";
    static final String SEND_VOTE_TOPIC_NAME = "vote-sink-topic";

    static final String RECEIVE_TEXT_TOPIC_NAME = "text-source-topic";
    static final String SEND_TEXT_TOPIC_NAME = "text-sink-topic";


    public static Properties of(String applicationName) {
        Properties props = new Properties();
        // 카프카 스트림즈를 유일하게 구분할 ID값
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);

        // 스트림즈에 접근할 카프카 broker 정보
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_LOCAL_BOOTSTRAP_SERVER);

        // 데이터를 어떤 형식으로 Read/Write 할지 성정 (키/값의 데이터 타입을 지정)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }
}
