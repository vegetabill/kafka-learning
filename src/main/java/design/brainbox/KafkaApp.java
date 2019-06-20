package design.brainbox;

import jdk.internal.util.EnvUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public abstract class KafkaApp
{
    private static final Logger logger = LoggerFactory.getLogger(KafkaApp.class);

    public void start() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServer());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        overrideConfig(config);

        StreamsBuilder builder = new StreamsBuilder();
        buildTopology(builder);
        Topology topology = builder.build();

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

        streams.cleanUp();
        streams.start();
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void overrideConfig(Properties config) {

    }

    protected abstract void buildTopology(StreamsBuilder builder);

    protected String getApplicationId() {
        return "app-" + this.getClass().getName();
    }

    protected String getBootstrapServer() {
        String bootstrapServer = EnvUtils.getEnvVar("BOOTSTRAP_SERVER");

        if (bootstrapServer != null && bootstrapServer.length() > 0) {
            return bootstrapServer;
        }

        return "localhost:9092";
    }

}
