package design.brainbox;

import design.brainbox.util.SimpleJson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;

import static design.brainbox.util.SimpleJson.parse;

public class RatingStats extends KafkaStreamsApp
{

    private static final Logger logger = LoggerFactory.getLogger(RatingStats.class);

    public static void main(String... args) {
        RatingStats app = new RatingStats();
        app.start();
    }

    private Float friendlyDivision(Number numerator, Number denominator) {
        Float n = numerator.floatValue();
        Float d = denominator.floatValue();

        if (n.isInfinite() || n.isNaN() || d.isInfinite() || d.isNaN()) {
            return 0.0f;
        }

        Float result = n / d;
        if (result.isNaN() || result.isInfinite()) {
            return 0.0f;
        }
        return result;
    }

    private static final DateFormat TIME_ONLY = new SimpleDateFormat("HH:mm:ss");
    public String friendlyWindowString(Windowed<String> w) {

        String start = TIME_ONLY.format(new Date(w.window().start()));
        String end = TIME_ONLY.format(new Date(w.window().end()));
        return String.format("[%s:%s/%s]", w.key(), start, end);
    }

    @Override
    protected void buildTopology(StreamsBuilder builder)
    {

        // by user

        KStream<String, String> userBookRatingActions = builder.stream("book-rating-user-actions",
                Consumed.with(Serdes.String(), Serdes.String()))
                .selectKey((key, value) -> {
                    SimpleJson json = parse(value);
                    return String.join("-", Arrays.asList(json.getString("user_id"), json.getString("book_id")));
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

        KTable<Windowed<String>, Long> ratingCountsPerWindow = userBookRatingActions
                .selectKey((key, value) -> {
                    {
                        Object rating = SimpleJson.parse(value).getRaw("rating");
                        if (rating == null)
                        {
                            return "0";
                        }
                        return rating.toString();
                    }
                })
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(30).toMillis()))
                .count();

        ratingCountsPerWindow
                .toStream()
                .map((windowed, value) -> KeyValue.pair(friendlyWindowString(windowed), value.toString()))
                .to("rating-counts-windowed");

        KTable<String, String> currentRatings = builder.table("user-ratings",
                Consumed.with(Serdes.String(), Serdes.String()));

        currentRatings
                .groupBy((key, value) -> KeyValue.pair(parse(value).getString("rating"), value))
                .count()
                .toStream()
                .map((key, value) -> KeyValue.pair(key, value.toString()))
                .to("all-ratings-distribution");

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
                = userRatingSums.join(userRatingCounts, this::friendlyDivision);

        userRatingAverages
                .mapValues((readOnlyKey, value) -> value.toString())
                .toStream()
                .to("user-rating-averages");

        /**
         * By Book
         *        .--.                   .---.
         *    .---|__|           .-.     |~~~|
         * .--|===|--|_          |_|     |~~~|--.
         * |  |===|  |'\     .---!~|  .--|   |--|
         * |%%|   |  |.'\    |===| |--|%%|   |  |
         * |%%|   |  |\.'\   |   | |__|  |   |  |
         * |  |   |  | \  \  |===| |==|  |   |  |
         * |  |   |__|  \.'\ |   |_|__|  |~~~|__|
         * |  |===|--|   \.'\|===|~|--|%%|~~~|--|
         * ^--^---'--^    `-'`---^-^--^--^---'--' hjw
         */
        KGroupedTable<String, String> currentBookRatings = currentRatings.groupBy((key, value) -> KeyValue.pair(parse(value).getString("book_id"), value),
                Serialized.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> bookRatingCounts = currentBookRatings.count();

        bookRatingCounts.
                toStream()
                .mapValues((readOnlyKey, value) -> value.toString())
                .to("book-rating-counts");

        KTable<String, Long> bookRatingSums = currentBookRatings.aggregate(
                () -> 0L,
                (key, value, aggregate) -> aggregate + parse(value).getNumber("rating").longValue(),
                (key, value, aggregate) -> aggregate - parse(value).getNumber("rating").longValue(),
                Materialized.with(Serdes.String(), Serdes.Long())
        );

        KTable<String, Float> bookRatingAverages =
                bookRatingSums
                        .join(bookRatingCounts, this::friendlyDivision);

        bookRatingAverages
                .mapValues((readOnlyKey, value) -> value.toString())
                .toStream()
                .to("book-rating-averages");

    }
}
