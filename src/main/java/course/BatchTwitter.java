package course;

import com.google.gson.Gson;
import course.util.dataclasses.TweetClass;

import course.util.mapper.streaming.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Class To Analyse Historic Data
 * reads files from sink at path
 */
public class BatchTwitter {

    public static void main(String[] args) throws Exception {

        final String path = "/home/yannik/IdeaProjects/big_data/";

        // set up the streaming execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        // create a configuration object
        Configuration parameters = new Configuration();

        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);

        DataSet<String> input = env.readTextFile(path + "sink_data/")
                .withParameters(parameters);

        //maps input to TweetClasses
        DataSet<TweetClass> tweets = input
                .flatMap(new FlatMapFunction<String, TweetClass>() {
                    @Override
                    public void flatMap(String json, Collector<TweetClass> collector) throws Exception {
                        Gson gson = new Gson();
                        collector.collect(gson.fromJson(json, TweetClass.class));
                    }
                });

        // gets All Batch Datasets
        DataSet<Tuple3<String, Integer, String>> wordCount = getWordCount(env, tweets);
        DataSet<Tuple2<Integer, String>> avgWordbyTweet = getAvgWordCountByTweet(env, tweets);
        DataSet<Tuple2<Integer, String>> avgTweetlength = getAvgTweetLength(env, tweets);
        DataSet<Tuple2<Integer, String>> avgRetweets = getAvgRetweets(env, tweets);
        DataSet<Tuple2<Integer, String>> avgFollower = getAvgFollower(env, tweets);
        DataSet<Tuple2<Integer, String>> maxWordLength = getMaxWordLength(env, tweets);

        //print
        wordCount.print();
        avgWordbyTweet.print();
        avgTweetlength.print();
        avgRetweets.print();
        avgFollower.print();
        maxWordLength.print();

        String time = Long.toString(System.currentTimeMillis());

        //save to csv
        wordCount.writeAsCsv(path + "batch_result/"+time+"/wordcount");
        avgWordbyTweet.writeAsCsv(path + "batch_result/"+time+"/avgWordbyTweet");
        avgTweetlength.writeAsCsv(path + "batch_result/"+time+"/avgTweetlength");
        avgRetweets.writeAsCsv(path + "batch_result/"+time+"/avgRetweets");
        avgFollower.writeAsCsv(path + "batch_result/"+time+"/avgFollower");
        maxWordLength.writeAsCsv(path + "batch_result/"+time+"/maxWordLength");

        env.execute("Twitter Batch");
    }

    /**
     * Gets the single Word Count
     *
     * @param env ExecutionEnvironment
     * @param text TweetClass Dataset
     * @return result with Tag
     * @throws Exception Exeption
     */
    public static DataSet<Tuple3<String, Integer, String>> getWordCount(ExecutionEnvironment env, DataSet<TweetClass> text) throws Exception {
        return text.flatMap(new WordCount())
                // group by the tuple field "0" and sum up tuple field "1"
                .groupBy(0,2)
                .sum(1);
    }

    /**
     * Gets Max Word Length
     *
     * @param env ExecutionEnvironment
     * @param text TweetClass Dataset
     * @return result with Tag
     * @throws Exception Exeption
     */
    public static DataSet<Tuple2<Integer, String>> getMaxWordLength(ExecutionEnvironment env, DataSet<TweetClass> text) throws Exception {
        return text.flatMap(new WordLength())
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
                     @Override
                     public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {
                         int max = 0;
                         String tag = "";
                         for (Tuple2<Integer, String> number : iterable) {
                             if (number.f0 > max) max = number.f0;
                             tag = number.f1;
                         }
                         collector.collect(new Tuple2<Integer, String>(max, tag));
                     }
                 });
    }

    /**
     * Gets Avg Word Count By Tweet
     *
     * @param env ExecutionEnvironment
     * @param text TweetClass Dataset
     * @return result with Tag
     * @throws Exception Exeption
     */
    public static DataSet<Tuple2<Integer, String>> getAvgWordCountByTweet(ExecutionEnvironment env, DataSet<TweetClass> text) throws Exception{

        return text.flatMap(new WordCountByTweet())
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
                        int sum = 0;
                        int count = 0;
                        String tag = "";
                        for (Tuple2<Integer, String> token : iterable) {
                            sum += token.f0;
                            count ++;
                            tag = token.f1;
                        }
                        collector.collect(new Tuple3<Integer, Integer, String>(count, sum, tag));
                    }
                }).map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(Tuple3<Integer, Integer, String> tuple) throws Exception {
                        return new Tuple2<Integer, String>(tuple.f1/tuple.f0, tuple.f2);
                    }
                });
    }

    /**
     * Gets Avg Tweetlength
     *
     * @param env ExecutionEnvironment
     * @param text TweetClass Dataset
     * @return result with Tag
     * @throws Exception Exeption
     */
    public static DataSet<Tuple2<Integer, String>> getAvgTweetLength(ExecutionEnvironment env, DataSet<TweetClass> text) throws Exception{

        return text.flatMap(new TweetLength())
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction< Tuple2<Integer, String>, Tuple3<Integer, Integer, String>>() {
                    @Override
                    public void reduce(Iterable< Tuple2<Integer, String>> iterable, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
                        int sum = 0;
                        int count = 0;
                        String tag = "";
                        for (Tuple2<Integer, String> token : iterable) {
                            sum += token.f0;
                            count ++;
                            tag = token.f1;
                        }
                        collector.collect(new Tuple3<Integer, Integer, String>(count, sum, tag));
                    }
                }).map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(Tuple3<Integer, Integer, String> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple.f1/tuple.f0, tuple.f2);
            }
        });
    }

    /**
     * Gets Avg Retweets
     *
     * @param env ExecutionEnvironment
     * @param text TweetClass Dataset
     * @return result with Tag
     * @throws Exception Exeption
     */
    public static DataSet<Tuple2<Integer, String>> getAvgRetweets(ExecutionEnvironment env, DataSet<TweetClass> text) throws Exception{

        return text.flatMap(new Retweets())
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
                        int sum = 0;
                        int count = 0;
                        String tag = "";
                        for (Tuple2<Integer, String> token : iterable) {
                            sum += token.f0;
                            count ++;
                            tag = token.f1;
                        }
                        collector.collect(new Tuple3<Integer, Integer, String>(count, sum, tag));
                    }
                }).map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(Tuple3<Integer, Integer, String> tuple) throws Exception {
                        return new Tuple2<Integer, String>(tuple.f1/tuple.f0, tuple.f2);
                    }
                });
    }

    /**
     * Gets Avg Follower
     *
     * @param env ExecutionEnvironment
     * @param text TweetClass Dataset
     * @return result with Tag
     * @throws Exception Exeption
     */
    public static DataSet<Tuple2<Integer, String>> getAvgFollower(ExecutionEnvironment env, DataSet<TweetClass> text) throws Exception{

        return text.flatMap(new FollowerCount())
                .groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {
                        int sum = 0;
                        int count = 0;
                        String tag = "";
                        for (Tuple2<Integer, String> token : iterable) {
                            sum += token.f0;
                            count ++;
                            tag = token.f1;
                        }
                        collector.collect(new Tuple3<Integer, Integer, String>(count, sum, tag));
                    }
                }).map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(Tuple3<Integer, Integer, String> tuple) throws Exception {
                        return new Tuple2<Integer, String>(tuple.f1/tuple.f0, tuple.f2);
                    }
                });
    }
}
