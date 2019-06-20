package design.brainbox;

import design.brainbox.util.SimpleJson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
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

        // by user

        KStream<String, String> userBookRatingActions = builder.stream("book-rating-user-actions",
                Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> {
                    SimpleJson json = parse(value);
                    return String.format("%s-%s", json.getString("user_id"), json.getString("book_id"));
                });

        userBookRatingActions
                .mapValues((readOnlyKey, value) -> {
                    Object rating = SimpleJson.parse(value).getRaw("rating");
                    if (rating == null) {
                        return null;
                    }
                    return value;
                })
                .to("user-ratings");

        KTable<String, String> currentRatings = builder.table("user-ratings",
                Consumed.with(Serdes.String(), Serdes.String()));

        KGroupedTable<String, String> currentUserRatings = currentRatings.groupBy((key, value) -> KeyValue.pair(parse(value).getString("user_id"), value),
                Serialized.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> userRatingCounts = currentUserRatings.count();

        userRatingCounts
            .toStream().mapValues((readOnlyKey, value) -> value.toString())
            .to("user-rating-counts");

        KTable<String, Long> userRatingSums = currentUserRatings.aggregate(
                () -> 0L,
                (key, value, aggregate) -> aggregate + parse(value).getNumber("rating").longValue(),
                (key, value, aggregate) -> aggregate - parse(value).getNumber("rating").longValue(),
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

//
//        // by book
//        KGroupedStream<String, String> bookRatings = builder.stream("book-rating-user-actions",
//                Consumed.with(Serdes.String(), Serdes.String()))
//                .selectKey((key, value) -> parse(value).getString("book_id"))
//                .groupByKey();
//
//        KTable<String, Long> bookRatingCounts = bookRatings.count();
//        bookRatingCounts.toStream().mapValues((readOnlyKey, value) -> value.toString())
//                .to("book-rating-counts");


    }
}
