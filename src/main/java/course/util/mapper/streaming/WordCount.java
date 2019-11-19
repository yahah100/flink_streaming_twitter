package course.util.mapper.streaming;

import course.util.dataclasses.TweetClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import java.util.Arrays;
import java.util.List;

/**
 *  Maps TweetClass to Tuple2(Word, Count, Tag) and removes a couple of Stopwords
 */
public final class WordCount implements FlatMapFunction<TweetClass, Tuple3<String, Integer, String>> {

    @Override
    public void flatMap(TweetClass tweetClass, Collector<Tuple3<String, Integer, String>> out) {
        // normalize and split the words
        String[] tokens = tweetClass.text.toLowerCase().split("\\W+");

        List<String> stopWords = loadStopwords();

        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0 && !stopWords.contains(token)) {
                out.collect(new Tuple3<String, Integer, String>(token, 1, tweetClass.tag));
            }
        }


    }
    public static List<String> loadStopwords(){
        String stopWords = "i me my myself we our ours ourselves you your yours yourself yourselves he him his " +
                "himself she her hers herself it its itself they them their theirs themselves what which who " +
                "whom this that these those am is are was were be been being have has had having do does did " +
                "doing a an the and but if or because as until while of at by for with about against between " +
                "into through during before after above below to from up down in out on off over under again " +
                "further then once here there when where why how all any both each few more most other some " +
                "such no nor not only own same so than too very s t can will just don should now http https co " +
                "re rt 1 2 3 4 5 6 7 8 9";

        return Arrays.asList(stopWords.split(" "));
    }
}