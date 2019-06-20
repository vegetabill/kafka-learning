package design.brainbox;

import design.brainbox.util.SimpleJson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static design.brainbox.util.SimpleJson.parse;

public class RatingStats extends KafkaApp
{

    private static final Logger logger = LoggerFactory.getLogger(RatingStats.class);

    public static void main(String... args) {
        RatingStats app = new RatingStats();
        app.start();
    }

    @Override
    protected void buildTopology(StreamsBuilder builder)
    {
        KGroupedStream<String, String> userBookRatings = builder.stream("book-rating-user-actions",
                Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> parse(value).getString("user_id"))
                .groupByKey();

        KTable<String, Long> userRatingCounts = userBookRatings.count();

        userRatingCounts.toStream().mapValues((readOnlyKey, value) -> value.toString())
                .to("user-rating-counts");

        KTable<String, Long> userRatingSums = userBookRatings
                .aggregate(() -> 0L,
                        (key, value, aggregate) -> aggregate + parse(value).getNumber("rating").longValue(),
                        Materialized.with(Serdes.String(), Serdes.Long()));

        userRatingSums.toStream()
                .mapValues((readOnlyKey, value) -> value.toString())
                .to("user-rating-sums", Produced.with(Serdes.String(), Serdes.String()));


        KTable<String, Float> userRatingAverages
                = userRatingSums.join(userRatingCounts, (sum, count) -> sum.floatValue() / count.floatValue());

        userRatingAverages
                .mapValues((readOnlyKey, value) -> value.toString())
                .toStream()
                .to("user-rating-averages");
    }
}
