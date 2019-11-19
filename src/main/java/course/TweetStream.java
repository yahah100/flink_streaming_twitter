package course;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import course.util.dataclasses.JsonNodeMapper;
import course.util.dataclasses.TweetClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;


/**
 * Class to Set up the Twitter DataStream, filter out 5 Topics, and pass all the Data in a TweetClass
 * (I tried to set up 5 different streams with one tag each to get the right tags, but when I did that I
 * just got Data from two Streams. So I did one stream and tagged it with the highest occurring tag of them)
 */
public class TweetStream {

    private static final List<String> Tags = Arrays.asList("Google", "Facebook", "Apple", "Intel", "Huawei");

    /**
     * Gets a Stream of TweetClasses with the Tags "Google", "Facebook", "Apple", "Intel", "Huawei"
     *
     * @param env StreamExecutionEnvironment
     * @return Datastream of TweetClasses
     */
    public static DataStream<TweetClass> getStream(StreamExecutionEnvironment env){
        Properties props = new Properties();

        try (InputStream input = new FileInputStream("/home/yannik/IdeaProjects/big_data/big_data/src/main/resources/config.properties")) {
            // load a properties file
            props.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        TwitterSource gTwitterSource = new TwitterSource(props);
        gTwitterSource.setCustomEndpointInitializer(new Filter());

        DataStream<String> gStreamSource = env.addSource(gTwitterSource);

        DataStream<TweetClass> allTweets = gStreamSource
                .flatMap(new SelectTweetClass());

        return allTweets;
    }


    public static class Filter implements TwitterSource.EndpointInitializer, Serializable {

        @Override
        public StreamingEndpoint createEndpoint() {
            StatusesFilterEndpoint statusesFilterEndpoint = new StatusesFilterEndpoint();
            statusesFilterEndpoint.trackTerms(Tags);
            return statusesFilterEndpoint;
        }
    }


    /**
     * Select the right data and converts the json into a TweetsClass
     */

    public static class SelectTweetClass implements FlatMapFunction<String, TweetClass> {

        private static final long serialVersionUID = 1L;

        private transient JsonNodeMapper jsonNodeMapper;

        /**
         * Select the language from the incoming JSON text.
         */
        @Override
        public void flatMap(String value, Collector<TweetClass> out) throws Exception {
            if (jsonNodeMapper == null){
                jsonNodeMapper = new JsonNodeMapper();
            }
            TweetClass tweetClass = jsonNodeMapper.createNewClass(value, getTag(value));
            if (tweetClass != null){
                out.collect(tweetClass);
            }
        }
    }

    /**
     * Chooses the Tag which occures the most in the json Input
     *
     * @param tweet String json to choose the tag
     * @return tag index
     */
    private static String getTag(String tweet){
        int counts[] = new int[5];

        IntStream.range(0, 5).forEach(
                i -> {
                    counts[i] = StringUtils.countMatches(tweet, Tags.get(i));
                }
        );

        int max = 0;
        int idx = 0;
        for (int i = 0; i < 5; i++){
            if (counts[i]> max){
                max = counts[i];
                idx = i;
            }
        }
        return Tags.get(idx);
    }

}
