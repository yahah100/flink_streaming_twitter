package course.util.mapper.streaming;

import course.util.dataclasses.TweetClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Maps TweetClass to Tuple2(FollowerCount, Tag)
 */
public class FollowerCount implements FlatMapFunction<TweetClass, Tuple2<Integer, String>> {

    @Override
    public void flatMap(TweetClass tweetClass, Collector<Tuple2<Integer, String>> out) {
        out.collect(new Tuple2<Integer, String>(tweetClass.followersCount, tweetClass.tag));
    }
}
