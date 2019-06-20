package design.brainbox;

import design.brainbox.util.SimpleJson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.java2d.pipe.SpanShapeRenderer;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

public class ExactlyOnceStreamProcessor extends KafkaStreamsApp
{
    private static final Logger logger = LoggerFactory.getLogger(ExactlyOnceStreamProcessor.class);

    public static void main(String[] args) {
        ExactlyOnceStreamProcessor processor = new ExactlyOnceStreamProcessor();
        processor.start();
    }

    public String extractMessageTimestamp(String message) {
        return SimpleJson.parse(message).getString("time");
    }

    public Date toDate(String s) {
        try
        {
            return ExactlyOnceProducer.DATE_FORMAT.parse(s);
        }
        catch (ParseException e)
        {
            throw new RuntimeException(e);
        }
    }

    public String toDateString(Date d) {
        return ExactlyOnceProducer.DATE_FORMAT.format(d);
    }

    public String maxDateString(String s1, String s2) {
        Date d1 = toDate(s1);
        Date d2 = toDate(s2);
        if (d1.compareTo(d2) > 0) {
            return toDateString(d1);
        } else {
            return toDateString(d2);
        }
    }

    @Override
    protected void buildTopology(StreamsBuilder builder)
    {

        KGroupedStream<String, String> customerTransactions = builder.stream(ExactlyOnceProducer.TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey();

        KTable<String, Long> customerBalances =  customerTransactions
                .aggregate(
                        () -> 0L,
                        (key, value, aggregate) -> aggregate + SimpleJson.parse(value).getNumber("amount").intValue(),
                        Materialized.with(Serdes.String(), Serdes.Long()));

        KTable<String, String> customerMostRecentTransactions = customerTransactions.aggregate(
                () -> toDateString(new Date(0)),
                (key, value, previousDateString) -> {
                    logger.info("aggregating data: " + value);
                    return maxDateString(extractMessageTimestamp(value), previousDateString);
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        customerMostRecentTransactions
                .toStream()
                .to("customer-most-recent-transactions");

        customerBalances
                .mapValues((readOnlyKey, value) -> value.toString())
                .toStream()
                .to("bank-account-balances");
    }
}
