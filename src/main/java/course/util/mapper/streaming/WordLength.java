package course.util.mapper.streaming;

import course.util.dataclasses.TweetClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Maps TweetClass to Tuple2(WordLengthbyTweet, Tag)
 */
public class WordLength implements FlatMapFunction<TweetClass, Tuple2<Integer, String>> {

    @Override
    public void flatMap(TweetClass tweetClass, Collector<Tuple2<Integer, String>> out) {
        String[] tokens = tweetClass.text.toLowerCase().split("\\W+");

        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<>(token.length(), tweetClass.tag));
            }
        }
    }
}