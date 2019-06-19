package design.brainbox;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount
{

    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    /**
     *
     * ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-inpu
     *
     * ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic word-count-inpu
     *
     * ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     *     --topic word-count-output \
     *     --from-beginning \
     *     --formatter kafka.tools.DefaultMessageFormatter \
     *     --property print.key=true \
     *     --property print.value=true \
     *     --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
     *     --property value.deserializer=org.apache.kafka.common.serialization.LongDeserialize
     *
     *
     *
     * @param args
     */
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCount wc = new WordCount();

        Topology topology = wc.createTopology();
        KafkaStreams streams = new KafkaStreams(topology, config);

        final CountDownLatch latch = new CountDownLatch(1);

        logger.info(topology.describe().toString());

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            logger.error("Error", e);
            System.exit(1);
        }
        System.exit(0);


    }

    private Topology createTopology()
    {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("word-count-input");

        KTable<String, Long> counts = input.mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((emptyKey, word) -> word)
                .groupByKey()
                .count();

        counts.toStream()
                .to("word-count-output",
                        Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
