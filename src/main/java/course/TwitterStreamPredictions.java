package course;


import course.predictions.*;
import course.util.dataclasses.TweetClass;
import course.util.mapper.streaming.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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


public class TwitterStreamPredictions {


    public static void main(String[] args) throws Exception {

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5).toMilliseconds()));
        env.setParallelism(5);

        DataStream<TweetClass> tweets = TweetStream.getStream(env);

        DataStream<Tuple4<Integer, Integer, String, String>> predictionTweetLength = tweets
                .flatMap(new TweetLength())
                .keyBy(1)
                .flatMap(new PredictionModel())
                .map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple4<Integer, Integer, String, String>>() {
                    @Override
                    public Tuple4<Integer, Integer, String, String> map(Tuple3<Integer, Integer, String> input) throws Exception {
                        return new Tuple4<>(input.f0, input.f1, input.f2, "LengthPredictions");
                    }
                });

        sinkPredictionData(predictionTweetLength, httpHosts);

        DataStream<Tuple4<Integer, Integer, String, String>> retweetPredictions = tweets
                .flatMap(new FollowerAndRetweets())
                .keyBy(2)
                .flatMap(new RetweetRegressionPredictionModel())
                .map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple4<Integer, Integer, String, String>>() {
                    @Override
                    public Tuple4<Integer, Integer, String, String> map(Tuple3<Integer, Integer, String> input) throws Exception {
                        return new Tuple4<>(input.f0, input.f1, input.f2, "RetweetPredictions");
                    }
                });

        sinkPredictionData(retweetPredictions, httpHosts);


        DataStream<Tuple4<Integer, Integer, String, String>> followerPredictions = tweets
                .flatMap(new StatusAndFollower())
                .keyBy(2)
                .flatMap(new FollowerRegressionPredictionModel())
                .map(new MapFunction<Tuple3<Integer, Integer, String>, Tuple4<Integer, Integer, String, String>>() {
                    @Override
                    public Tuple4<Integer, Integer, String, String> map(Tuple3<Integer, Integer, String> input) throws Exception {
                        return new Tuple4<>(input.f0, input.f1, input.f2, "FollowerPredictions");
                    }
                });

        sinkPredictionData(followerPredictions, httpHosts);

        // execute program
        env.execute("Streaming Incremental Learning");
    }

    /**
     * Model with a State which tries to predict the Follower count with the status Count as input
     */
    public static class FollowerRegressionPredictionModel extends RichFlatMapFunction< Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {

        private transient ValueState<SimpleRegressionPredictionModel> modelState;
        private transient ValueState<Integer> countState;


        @Override
        public void flatMap( Tuple3<Integer, Integer, String> input, Collector<Tuple3<Integer, Integer, String>> out) throws Exception {

            // fetch operator state
            SimpleRegressionPredictionModel model = modelState.value();
            if (model == null) {
                model = new SimpleRegressionPredictionModel();
            }
            int count;
            if (countState.value() == null) {
                count = 0;
            }else{
                count = countState.value();
            }
            count ++;

            // Model starts to predict when it got 20 or more data points
            if (count>20){
                int predictedretweets = model.predict(input.f0, input.f2);

                out.collect(new Tuple3<Integer, Integer, String>(predictedretweets, input.f1, input.f2));
            }

            model.refineModel(input.f0, input.f1, input.f2);

            modelState.update(model);
            countState.update(count);
        }


        @Override
        public void open(Configuration config) {
            // obtain key-value state for prediction model
            ValueStateDescriptor<SimpleRegressionPredictionModel> descriptor = new ValueStateDescriptor<>(
                    "regressionModel",
                    TypeInformation.of(SimpleRegressionPredictionModel.class));

            modelState = getRuntimeContext().getState(descriptor);
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count State", Integer.class));
        }
    }

    /**
     * Model with a State which tries to predict the Retweet count with the Follower Count as input
     */
    public static class RetweetRegressionPredictionModel extends RichFlatMapFunction<Tuple3<Integer, Integer, String>, Tuple3<Integer, Integer, String>> {

        private transient ValueState<SimpleRegressionPredictionModel> modelState;
        private transient ValueState<Integer> countState;


        @Override
        public void flatMap( Tuple3<Integer, Integer, String> input, Collector<Tuple3<Integer, Integer, String>> out) throws Exception {

            // fetch operator state
            SimpleRegressionPredictionModel model = modelState.value();
            if (model == null) {
                model = new SimpleRegressionPredictionModel();
            }
            int count;
            if (countState.value() == null) {
                count = 0;
            }else{
                count = countState.value();
            }
            count ++;

            // Model starts to predict when it got 20 or more data points
            if (count>20){
                int predictedretweets = model.predict(input.f0, input.f2);

                out.collect(new Tuple3<Integer, Integer, String>(predictedretweets, input.f1, input.f2));
            }

            model.refineModel(input.f0, input.f1, input.f2);

            modelState.update(model);
            countState.update(count);
        }


        @Override
        public void open(Configuration config) {
            // obtain key-value state for prediction model
            ValueStateDescriptor<SimpleRegressionPredictionModel> descriptor = new ValueStateDescriptor<>(
                    "regressionModel",
                    TypeInformation.of(SimpleRegressionPredictionModel.class));

            modelState = getRuntimeContext().getState(descriptor);
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count State", Integer.class));
        }
    }

    /**
     * Model with a State which tries to predict the TweetLength with a Simple Method of predicting
     */
    public static class PredictionModel extends RichFlatMapFunction<Tuple2<Integer, String>, Tuple3<Integer, Integer, String>> {

        private transient ValueState<SimplePrediction> modelState;

        @Override
        public void flatMap( Tuple2<Integer, String> length, Collector<Tuple3<Integer, Integer, String>> out) throws Exception {

            // fetch operator state
            SimplePrediction model = modelState.value();
            if (model == null) {
                model = new SimplePrediction(0.1);
            }

            int predictedLength = model.predict(length.f0, length.f1);

            out.collect(new Tuple3<Integer, Integer, String>(predictedLength, length.f0, length.f1));

            model.refineModel(length.f0, length.f1);

            modelState.update(model);
        }


        @Override
        public void open(Configuration config) {
            // obtain key-value state for prediction model
            ValueStateDescriptor<SimplePrediction> descriptor = new ValueStateDescriptor<>("avgModel", TypeInformation.of(SimplePrediction.class));

            modelState = getRuntimeContext().getState(descriptor);
        }
    }

    /**
     * Writes Predicted results to a String
     *
     * @param ds Datastream of results
     * @param httpHosts List of address to ES
     */
    private static void sinkPredictionData(DataStream<Tuple4<Integer, Integer, String, String>> ds, List<HttpHost> httpHosts){

        ElasticsearchSink.Builder<Tuple4<Integer, Integer, String, String>> esCompareDataSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple4<Integer, Integer, String, String>>() {
                    public IndexRequest createIndexRequest(Tuple4<Integer, Integer, String, String> element) {

                        Map json = new HashMap<>();
                        json.put("predictions_predicted", element.f0);
                        json.put("predictions_real", element.f1);
                        json.put("predictions_tag", element.f2);
                        json.put("predictions_name", element.f3);
                        json.put("predictions_timestamp", System.currentTimeMillis());


                        return Requests.indexRequest()
                                .index("tw_predictions")
                                .type("predictions")
                                .source(json);
                    }
                    @Override
                    public void process(Tuple4<Integer, Integer, String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esCompareDataSinkBuilder.setBulkFlushMaxActions(1);
        ds.addSink(esCompareDataSinkBuilder.build());
    }
}
