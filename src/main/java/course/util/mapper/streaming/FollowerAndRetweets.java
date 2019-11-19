package course.util.mapper.streaming;

import course.util.dataclasses.TweetClass;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 *  Maps TweetClass to Tuple3(RetweetCount, FollowerCount. Tag)
 */
public class FollowerAndRetweets implements FlatMapFunction<TweetClass, Tuple3<Integer, Integer, String>> {

    @Override
    public void flatMap(TweetClass tweetClass, Collector<Tuple3<Integer, Integer, String>> out) {
        if (tweetClass.retweet.id != 0L){
            out.collect(new Tuple3<Integer, Integer, String>(tweetClass.retweet.retweetCount, tweetClass.retweet.followersCount, tweetClass.tag));
        }

    }
}
