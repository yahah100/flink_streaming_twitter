package course.util.mapper.streaming;

import course.util.dataclasses.TweetClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 *  Maps TweetClass to Tuple2(TweetLength, Tag)
 */
public class TweetLength  implements FlatMapFunction<TweetClass, Tuple2<Integer, String>> {

    @Override
    public void flatMap(TweetClass tweetClass, Collector<Tuple2<Integer, String>> out) {
        if (tweetClass.text.length() > 0){
            out.collect(new Tuple2<Integer, String>(tweetClass.text.length(), tweetClass.tag));
        }
    }
}