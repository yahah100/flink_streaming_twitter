package course;

import com.google.gson.Gson;
import course.util.dataclasses.TweetClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for Comparision of Historic Data and realtime streaming Data
 * uses the Functions of BatchTwitter and TwitterStreamingJob and dumps the result to
 * a Elasticsearch sink
 *
 */
public class CompareBatchWithStreaming {

    public static void main(String[] args) throws Exception {

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        // set up the streaming execution environment
        final StreamExecutionEnvironment envhistoric = StreamExecutionEnvironment.getExecutionEnvironment();


        TextInputFormat textInputFormat = new TextInputFormat(new Path("/home/yannik/IdeaProjects/big_data/sink_data/"));
        textInputFormat.setNestedFileEnumeration(true);

        DataStream<String> historicInput = envhistoric.readFile(textInputFormat, "/home/yannik/IdeaProjects/big_data/sink_data/");


        // maps input to TweetClasses
        DataStream<TweetClass> oldtweets = historicInput
                .flatMap(new FlatMapFunction<String, TweetClass>() {
                    @Override
                    public void flatMap(String json, Collector<TweetClass> collector) throws Exception {
                        Gson gson = new Gson();
                        collector.collect(gson.fromJson(json, TweetClass.class));
                    }
                });



        //loads all Batch Datasets
//        DataSet<Tuple3<String, Integer, String>> historicWordCount = BatchTwitter.getWordCount(envBatch, oldtweets);
        DataStream<Tuple2<Double, String>>  historicAvgWordCountByTweet = TwitterStreamingJob.getAvgWordInTweetHistoric(oldtweets).keyBy(1);
        DataStream<Tuple2<Double, String>> historicAvgTweetLength = TwitterStreamingJob.getAvgTweetLengthHistoric(oldtweets).keyBy(1);
        DataStream<Tuple2<Double, String>> historicAvgRetweets = TwitterStreamingJob.getAvgReRetweetsHistoric(oldtweets).keyBy(1);
        DataStream<Tuple2<Double, String>> historicAvgFollower = TwitterStreamingJob.getAvgFollowerCountHistoric(oldtweets).keyBy(1);
        DataStream<Tuple2<Integer, String>> historicMaxWordLength = TwitterStreamingJob.getMaxWordLengthHistoric(oldtweets);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5).toMilliseconds()));

        DataStream<TweetClass> tweets = TweetStream.getStream(env);

        //Gets all the datastreams
        DataStream<Tuple2<Double, String>> streamAvgWordInTweet = TwitterStreamingJob.getAvgWordInTweet(tweets);
        DataStream<Tuple2<Integer, String>> streamMaxWordLength = TwitterStreamingJob.getMaxWordLength(tweets);
        DataStream<Tuple2<Double, String>> streamAvgFollowerCount = TwitterStreamingJob.getAvgFollowerCount(tweets);
        DataStream<Tuple2<Double, String>> streamAvgReRetweets = TwitterStreamingJob.getAvgReRetweets(tweets);
        DataStream<Tuple2<Double, String>> streamAvgTweetLength = TwitterStreamingJob.getAvgTweetLength(tweets);


        //
        //Compares the Streaming Data with the historic result
        //
        DataStream<Tuple2<Integer, String>> diffMaxWordLength = streamMaxWordLength
                .keyBy(1)
                .connect(historicMaxWordLength.keyBy(1))
                .flatMap(new DiffIntEnrichmentFunction());

        diffMaxWordLength.print();

        intSinkCompareData(diffMaxWordLength, httpHosts, "maxWordLength");


        DataStream<Tuple2<Double, String>> diffAvgTweetLength = streamAvgTweetLength
                .keyBy(1)
                .connect(historicAvgTweetLength.keyBy(1))
                .flatMap(new DiffEnrichmentFunction());

        doubleSinkCompareData(diffAvgTweetLength, httpHosts, "avgTweetLength");


        DataStream<Tuple2<Double, String>> diffAvgRetweets = streamAvgReRetweets
                .keyBy(1)
                .connect(historicAvgRetweets.keyBy(1))
                .flatMap(new DiffEnrichmentFunction());

        doubleSinkCompareData(diffAvgRetweets, httpHosts, "avgRetweets");


        DataStream<Tuple2<Double, String>> diffAvgFollowerCount = streamAvgFollowerCount
                .keyBy(1)
                .connect(historicAvgFollower.keyBy(1))
                .flatMap(new DiffEnrichmentFunction());

        doubleSinkCompareData(diffAvgFollowerCount, httpHosts, "avgFollowerCount");


        DataStream<Tuple2<Double, String>> diffAvgWordInTweet = streamAvgWordInTweet
                .keyBy(1)
                .connect(historicAvgWordCountByTweet.keyBy(1))
                .flatMap(new DiffEnrichmentFunction());

        doubleSinkCompareData(diffAvgWordInTweet, httpHosts, "avgWordInTweet");


        envhistoric.execute("Twitter Batch");
        env.execute("Twitter Streaming");

    }


    /**
     * Diff Function with valuestate of the historic data
     */
    public static class DiffEnrichmentFunction extends RichCoFlatMapFunction<Tuple2<Double, String>, Tuple2<Double, String>, Tuple2<Double, String>> {
        // keyed, managed state
        private ValueState<Tuple2<Double, String>> historicState;

        @Override
        public void open(Configuration config) {
            historicState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "saved ride",
                    TypeInformation.of(new TypeHint<Tuple2<Double, String>>() {}),
                    Tuple2.of(0d, "")));

        }

        /**
         * Updates the Valuestate
         *
         * @param doubleStringTuple2 historic data stream
         * @param collector does not collect anything
         * @throws Exception Exception
         */
        @Override
        public void flatMap2(Tuple2<Double, String> doubleStringTuple2, Collector<Tuple2<Double, String>> collector) throws Exception {
            Tuple2<Double, String> state = historicState.value();

            if (state != null) {
                historicState.clear();
            }
            System.out.println("------------- updates State " + doubleStringTuple2.f1 + "-----------");
            historicState.update(doubleStringTuple2);
        }

        /**
         * Compares real time streaming data with the valuestate
         *
         * @param doubleStringTuple2 real time data stream
         * @param collector collects the diffrence from State and realtime data
         * @throws Exception Exception
         */
        @Override
        public void flatMap1(Tuple2<Double, String> doubleStringTuple2, Collector<Tuple2<Double, String>> collector) throws Exception {
            Tuple2<Double, String> state = historicState.value();

            if (state.f1.equals(doubleStringTuple2.f1)){
                collector.collect(new Tuple2<>((state.f0 - doubleStringTuple2.f0), doubleStringTuple2.f1));
            }else System.out.println("Key by error");
        }
    }

    /**
     * Diff Function with valuestate of the historic data
     */
    public static class DiffIntEnrichmentFunction extends RichCoFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple2<Integer, String>> {
        // keyed, managed state
        private ValueState<Tuple2<Integer, String>> historicState;

        @Override
        public void open(Configuration config) {
            historicState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "saved ride",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {}),
                    Tuple2.of(0, "")));

        }

        /**
         * Updates the Valuestate
         *
         * @param integrStringTuple2 historic data stream
         * @param collector does not collect anything
         * @throws Exception Exception
         */
        @Override
        public void flatMap2(Tuple2<Integer, String> integrStringTuple2, Collector<Tuple2<Integer, String>> collector) throws Exception {
            Tuple2<Integer, String> state = historicState.value();

            if (state != null) {
                historicState.clear();
            }
            System.out.println("------------- updates State " + integrStringTuple2.f1 + "-----------");
            historicState.update(integrStringTuple2);
        }

        /**
         * Compares real time streaming data with the valuestate
         *
         * @param integrStringTuple2 real time data stream
         * @param collector collects the diffrence from State and realtime data
         * @throws Exception Exception
         */
        @Override
        public void flatMap1(Tuple2<Integer, String> integrStringTuple2, Collector<Tuple2<Integer, String>> collector) throws Exception {
            Tuple2<Integer, String> state = historicState.value();

            if (state.f1.equals(integrStringTuple2.f1)){
                collector.collect(new Tuple2<>((int) (state.f0 - integrStringTuple2.f0), integrStringTuple2.f1));
            }else System.out.println("Key by error: " + integrStringTuple2.f1 + " " + state.f1);
        }
    }

    private static void doubleSinkCompareData(DataStream<Tuple2<Double, String>> ds, List<HttpHost> httpHosts, String name){

        ElasticsearchSink.Builder<Tuple2<Double, String>> esCompareDataSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<Double, String>>() {
                    public IndexRequest createIndexRequest(Tuple2<Double, String> element) {

                        Map json = new HashMap<>();
                        json.put("comparedata", element.f0);
                        json.put("comparedata_tag", element.f1);
                        json.put("comparedata_name", name);
                        json.put("comparedata_timestamp", System.currentTimeMillis());


                        return Requests.indexRequest()
                                .index("tw_comparedata")
                                .type("comparedata")
                                .source(json);
                    }
                    @Override
                    public void process(Tuple2<Double, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esCompareDataSinkBuilder.setBulkFlushMaxActions(1);
        ds.addSink(esCompareDataSinkBuilder.build());
    }

    private static void intSinkCompareData(DataStream<Tuple2<Integer, String>> ds, List<HttpHost> httpHosts, String name){

        ElasticsearchSink.Builder<Tuple2<Integer,String>> esCompareDataSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<Integer, String>>() {
                    public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {

                        Map json = new HashMap<>();
                        json.put("comparedata", element.f0);
                        json.put("comparedata_tag", element.f1);
                        json.put("comparedata_name", name);
                        json.put("comparedata_timestamp", System.currentTimeMillis());


                        return Requests.indexRequest()
                                .index("tw_comparedata")
                                .type("comparedata")
                                .source(json);
                    }
                    @Override
                    public void process(Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esCompareDataSinkBuilder.setBulkFlushMaxActions(1);
        ds.addSink(esCompareDataSinkBuilder.build());
    }

    private static Map<String, Integer> intListToMAp(List<Tuple2<Integer, String>> list){
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Tuple2<Integer, String> item : list){
            map.put(item.f1, item.f0);
        }
        return map;
    }

    private static Map<String, Double> doubleListToMAp(List<Tuple2<Integer, String>> list){
        Map<String, Double> map = new HashMap<String, Double>();
        for (Tuple2<Integer, String> item : list){
            map.put(item.f1,(double) item.f0);
        }
        return map;
    }


}
