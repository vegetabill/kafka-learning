package design.brainbox;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class FavouriteColour
{
    private static final Logger logger = LoggerFactory.getLogger(FavouriteColour.class);

    /**

     */
    public static void main(String[] args)
    {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        FavouriteColour app = new FavouriteColour();

        Topology topology = app.createTopology();
        KafkaStreams streams = new KafkaStreams(topology, config);

        final CountDownLatch latch = new CountDownLatch(1);

        logger.info(topology.describe().toString());

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                latch.countDown();
            }
        });

        try
        {
            streams.cleanUp();
            streams.start();
            latch.await();
        }
        catch (Throwable e)
        {
            logger.error("Error", e);
            System.exit(1);
        }
        System.exit(0);


    }

    private static final Set<String> VALID_COLOURS = new HashSet<>(Arrays.asList("red", "green", "blue"));

    private Topology createTopology()
    {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("favourite-colour-input");

        input.filter((blank, line) -> line.contains(",") && line.length() > 3)
                .selectKey((blank, line) -> line.split(",")[0])
                .mapValues((user, line) -> line.split(",")[1])
                .filter((user, colour) -> VALID_COLOURS.contains(colour))
                .to("favourite-colour-updates");

        KTable<String, String> currentFaves = builder.table("favourite-colour-updates",
                Consumed.with(Serdes.String(), Serdes.String()));

        currentFaves.groupBy((user, colour) -> KeyValue.pair(colour, colour))
                .count()
                .toStream()
                .to("favourite-colour-counts", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
