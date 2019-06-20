package design.brainbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class ExactlyOnceStreamProcessor extends KafkaStreamsApp
{
    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceStreamProcessor.class);

    public static void main(String[] args) {
        ExactlyOnceStreamProcessor processor = new ExactlyOnceStreamProcessor();
        processor.start();
    }

    public JsonNode buildJson(Integer count, Integer balance, Date date) {
        ObjectNode record = JsonNodeFactory.instance.objectNode();
        record.put("count", count);
        record.put("balance", balance);
        record.put("time", date.getTime());
        return record;
    }

    @Override
    protected void buildTopology(StreamsBuilder builder)
    {


        KGroupedStream<String, JsonNode> customerTransactions =
                builder.stream(ExactlyOnceProducer.TOPIC,
                Consumed.with(Serdes.String(), JSON_SERDE))
                .groupByKey(Serialized.with(Serdes.String(), JSON_SERDE));

        JsonNode emptyCustomerRecord = buildJson(1, 0, new Date(0));

        KTable<String, JsonNode> customerBalances =  customerTransactions
                .aggregate(
                        () -> emptyCustomerRecord,
                        (key, value, aggregate) -> {
                            logger.info(value.getClass().getName() + " aggregating: " + value.toString());
                            return updatedRecord(value, aggregate);
//                            return value.toString();
                        },
                        Materialized.with(Serdes.String(), JSON_SERDE));

        customerBalances
                .toStream()
                .to("account-status-json",
                        Produced.with(Serdes.String(), JSON_SERDE));
    }

    private JsonNode updatedRecord(JsonNode transaction, JsonNode balance)
    {
        Integer newCount = balance.get("count").asInt() + 1;
        Integer newBalance = transaction.get("amount").asInt() + balance.get("balance").asInt();
        Long processingMillis = Math.max(transaction.get("time").asLong(), balance.get("time").asLong());
        return buildJson(newCount, newBalance, new Date(processingMillis));
    }

    @Override
    protected void overrideConfig(Properties config)
    {
        super.overrideConfig(config);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    }
}
