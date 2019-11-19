package course.util.mapper.streaming;

import course.util.dataclasses.TweetClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 *  Maps TweetClass to Tuple3(StatusCount, FollowerCount, Tag)
 */
public class StatusAndFollower implements FlatMapFunction<TweetClass, Tuple3<Integer, Integer, String>> {

    @Override
    public void flatMap(TweetClass tweetClass, Collector<Tuple3<Integer, Integer, String>> out) {
        if (tweetClass.id != 0L){
            out.collect(new Tuple3<Integer, Integer, String>(tweetClass.statusesCount, tweetClass.followersCount, tweetClass.tag));
        }

    }
}
