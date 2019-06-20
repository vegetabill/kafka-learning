package design.brainbox;

import design.brainbox.util.SimpleJson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MostReadBooks extends KafkaApp
{
    private static final Logger logger = LoggerFactory.getLogger(MostReadBooks.class);

    public static void main(String... args) {
        MostReadBooks app = new MostReadBooks();
        app.start();
    }

    @Override
    protected void buildTopology(StreamsBuilder builder)
    {
        KGroupedStream<String, String> bookRatings = builder.stream("book-rating-user-actions",
                Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> SimpleJson.parse(value).getString("book_id"))
                .groupByKey();



        // ruby client can only handle strings
        bookRatings.count().mapValues((readOnlyKey, value) -> value.toString())
                .toStream()
                .to("book-rating-counts", Produced.with(Serdes.String(), Serdes.String()));
    }
}
