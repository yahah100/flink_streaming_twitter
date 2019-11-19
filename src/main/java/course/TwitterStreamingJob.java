/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package course;

import com.google.gson.Gson;
import course.util.dataclasses.TweetClass;
import course.util.mapper.streaming.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.util.*;

/**
 * Flink Streaming Job.
 *
 * Stream data and put the data into a sink for elasticsearch
 */
public class TwitterStreamingJob {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(5000L);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5).toMilliseconds()));

		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

		ElasticsearchSink.Builder<TweetClass> esSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<TweetClass>() {
					public IndexRequest createIndexRequest(TweetClass element) {

						Gson gson = new Gson();
						String json = gson.toJson(element);
						Map map = gson.fromJson(json, Map.class);
						return Requests.indexRequest()
								.index("twitter")
								.type("tweet")
								.source(map);
					}

					@Override
					public void process(TweetClass element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		esSinkBuilder.setBulkFlushMaxActions(1);
		DataStream<TweetClass> tweets = TweetStream.getStream(env);

		tweets.addSink(esSinkBuilder.build());

		//write data to sink for local analysation
		writeDataToLocalSink(tweets);

		wordCount(tweets, httpHosts);

		// 5 statistics with sink to elasticsearch
		avgWordInTweet(tweets, httpHosts);
		avgReRetweets(tweets, httpHosts);
		avgTweetLength(tweets, httpHosts);
		avgFollowerCount(tweets, httpHosts);
		maxWordLength(tweets, httpHosts);

		env.execute("Twitter Streaming");
	}

	/**
	 * Gets Max Wordlength in Window of 30secs
	 * is keyed by the tag
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Integer, String>> getMaxWordLength(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new WordLength())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> integer) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
				.max(0);
	}

	/**
	 * Gets Max Wordlength in Window of Session Window,
	 * The SessionWindow is triggerd if it does not get any Data anymore for a time interval
	 * is keyed by the tag
	 *
	 * If you have a lot of data which takes to long to process all, you can uncomment the TumblingProcessingTimeWindows
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Integer, String>> getMaxWordLengthHistoric(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new WordLength())
				.keyBy(1)
				//.window(TumblingProcessingTimeWindows.of(Time.minutes(15)))
				.window(EventTimeSessionWindows.withGap(Time.seconds(1)))
				.max(0);
	}
	/**
	 * Sink to elastic Search
	 *
	 * @param tweets Datastream of TweetClasses
	 * @param httpHosts List for elasticsearch address
	 */
	private static void maxWordLength(DataStream<TweetClass> tweets, List<HttpHost> httpHosts){
		DataStream<Tuple2<Integer, String>> avgTweetLength = getMaxWordLength(tweets);

		ElasticsearchSink.Builder<Tuple2<Integer, String>> esMaxWordLengthSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Tuple2<Integer, String>>() {
					public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {

						Map json = new HashMap<>();
						json.put("max_wordlength", element.f0);
						json.put("max_wordlength_tag", element.f1);
						json.put("max_wordlength_timestamp", System.currentTimeMillis());


						return Requests.indexRequest()
								.index("tw_max_wordlength")
								.type("max_wordlength")
								.source(json);
					}
					@Override
					public void process(Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		esMaxWordLengthSinkBuilder.setBulkFlushMaxActions(1);
		avgTweetLength.addSink(esMaxWordLengthSinkBuilder.build());
	}

	/**
	 * Gets AVg Tweetlength in Window of 30secs
	 * is keyed by the tag
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgTweetLength(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new TweetLength())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
				.aggregate(new AverageAggregateTag());

	}

	/**
	 * Gets AVG Tweetlength in a Session Window,
	 * The SessionWindow is triggerd if it does not get any Data anymore for a time interval
	 * is keyed by the tag
	 *
	 * If you have a lot of data which takes to long to process all, you can uncomment the TumblingProcessingTimeWindows
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgTweetLengthHistoric(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new TweetLength())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(EventTimeSessionWindows.withGap(Time.seconds(1)))
				//.window(TumblingProcessingTimeWindows.of(Time.minutes(15)))
				.aggregate(new AverageAggregateTag());

	}

	/**
	 * Writes th Data to Elastic Search
	 *
	 * @param tweets Datastream of TweetClasses
	 * @param httpHosts List of Address to ES
	 */
	private static void avgTweetLength(DataStream<TweetClass> tweets, List<HttpHost> httpHosts){
		DataStream<Tuple2<Double, String>> avgTweetLength = getAvgTweetLength(tweets);

		ElasticsearchSink.Builder<Tuple2<Double, String>> esAvgTweetLengthSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Tuple2<Double, String>>() {
					public IndexRequest createIndexRequest(Tuple2<Double, String> element) {

						Map json = new HashMap<>();
						json.put("avg_tweetlength", element.f0);
						json.put("avg_tweetlength_tag", element.f1);
						json.put("avg_tweetlength_timestamp", System.currentTimeMillis());


						return Requests.indexRequest()
								.index("tw_avg_tweetlength")
								.type("avg_tweetlength")
								.source(json);
					}
					@Override
					public void process(Tuple2<Double, String> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		esAvgTweetLengthSinkBuilder.setBulkFlushMaxActions(1);
		avgTweetLength.addSink(esAvgTweetLengthSinkBuilder.build());
	}

	/**
	 * Gets AVG Follower Count in Window of 30secs
	 * is keyed by the tag
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgFollowerCount(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new FollowerCount())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
				.aggregate(new AverageAggregateTag());
	}

	/**
	 * Gets AVG Follower Count of Session Window,
	 * The SessionWindow is triggerd if it does not get any Data anymore for a time interval
	 * is keyed by the tag
	 *
	 * If you have a lot of data which takes to long to process all, you can uncomment the TumblingProcessingTimeWindows
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgFollowerCountHistoric(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new FollowerCount())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(EventTimeSessionWindows.withGap(Time.seconds(1)))
				//.window(TumblingProcessingTimeWindows.of(Time.minutes(15)))
				.aggregate(new AverageAggregateTag());
	}

	/**
	 * Writes th Data to Elastic Search
	 *
	 * @param tweets Datastream of TweetClasses
	 * @param httpHosts List of Address to ES
	 */
	private static void  avgFollowerCount(DataStream<TweetClass> tweets, List<HttpHost> httpHosts){
		DataStream<Tuple2<Double, String>> avgFollowerCount = getAvgFollowerCount(tweets);

		ElasticsearchSink.Builder<Tuple2<Double, String>> esAvgFollowerCountSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Tuple2<Double, String>>() {
					public IndexRequest createIndexRequest(Tuple2<Double, String> element) {

						Map json = new HashMap<>();
						json.put("avg_follower_count", element.f0);
						json.put("avg_follower_count_tag", element.f1);
						json.put("avg_follower_count_timestamp", System.currentTimeMillis());


						return Requests.indexRequest()
								.index("tw_avg_follower_count")
								.type("avg_follower_count")
								.source(json);
					}
					@Override
					public void process(Tuple2<Double, String> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		esAvgFollowerCountSinkBuilder.setBulkFlushMaxActions(1);
		avgFollowerCount.addSink(esAvgFollowerCountSinkBuilder.build());
	}

	/**
	 * Gets AVG ReRetweets Count in Window of 30secs
	 * is keyed by the tag
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgReRetweets(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new Retweets())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
				.aggregate(new AverageAggregateTag());
	}

	/**
	 * Gets AVG ReRetweets Count of Session Window,
	 * The SessionWindow is triggerd if it does not get any Data anymore for a time interval
	 * is keyed by the tag
	 *
	 * If you have a lot of data which takes to long to process all, you can uncomment the TumblingProcessingTimeWindows
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgReRetweetsHistoric(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new Retweets())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(EventTimeSessionWindows.withGap(Time.seconds(1)))
				//.window(TumblingProcessingTimeWindows.of(Time.minutes(15)))
				.aggregate(new AverageAggregateTag());
	}



	/**
	 * Writes th Data to Elastic Search
	 *
	 * @param tweets Datastream of TweetClasses
	 * @param httpHosts List of Address to ES
	 */
	private static void  avgReRetweets(DataStream<TweetClass> tweets, List<HttpHost> httpHosts){
		DataStream<Tuple2<Double, String>> avgRetweets = getAvgReRetweets(tweets);

		ElasticsearchSink.Builder<Tuple2<Double, String>> esAvgRetweetsSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Tuple2<Double, String>>() {
					public IndexRequest createIndexRequest(Tuple2<Double, String> element) {

						Map json = new HashMap<>();
						json.put("avg_reretweets",  element.f0);
						json.put("avg_reretweets_tag",  element.f1);
						json.put("avg_reretweets_timestamp", System.currentTimeMillis());


						return Requests.indexRequest()
								.index("tw_avg_reretweets")
								.type("avg_reretweets")
								.source(json);
					}
					@Override
					public void process(Tuple2<Double, String> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		esAvgRetweetsSinkBuilder.setBulkFlushMaxActions(1);
		avgRetweets.addSink(esAvgRetweetsSinkBuilder.build());
	}

	/**
	 * Gets AVG Word Count in one Tweet in Window of 30secs
	 * is keyed by the tag
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgWordInTweet(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new WordCountByTweet())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
				.aggregate(new AverageAggregateTag());
	}

	/**
	 * Gets AVG Word Count in Window of Session Window,
	 * The SessionWindow is triggerd if it does not get any Data anymore for a time interval
	 * is keyed by the tag
	 *
	 * If you have a lot of data which takes to long to process all, you can uncomment the TumblingProcessingTimeWindows
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple2<Double, String>> getAvgWordInTweetHistoric(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new WordCountByTweet())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(1)
				.window(EventTimeSessionWindows.withGap(Time.seconds(1)))
				//.window(TumblingProcessingTimeWindows.of(Time.minutes(15)))
				.aggregate(new AverageAggregateTag());
	}
	/**
	 * Writes th Data to Elastic Search
	 *
	 * @param tweets Datastream of TweetClasses
	 * @param httpHosts List of Address to ES
	 */
	private static void avgWordInTweet(DataStream<TweetClass> tweets, List<HttpHost> httpHosts){
		DataStream<Tuple2<Double, String>> avgWordsInTweet = getAvgWordInTweet(tweets);

		ElasticsearchSink.Builder<Tuple2<Double, String>> esAvgWordsInTweetSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Tuple2<Double, String>>() {
					public IndexRequest createIndexRequest(Tuple2<Double, String> element) {

						Map json = new HashMap<>();
						json.put("avgWordsInTweet",  element.f0);
						json.put("avgWordsInTweet_tag",  element.f1);
						json.put("avg_words_in_tweet_timestamp", System.currentTimeMillis());


						return Requests.indexRequest()
								.index("tw_avg_words_in_tweet")
								.type("avg_words_in_tweet")
								.source(json);
					}
					@Override
					public void process(Tuple2<Double, String> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		esAvgWordsInTweetSinkBuilder.setBulkFlushMaxActions(1);
		avgWordsInTweet.addSink(esAvgWordsInTweetSinkBuilder.build());
	}

	/**
	 * Gets Word Count in Window of 60secs
	 * is keyed by the word and tag
	 *
	 * @param tweets Datastream of TweetClasses
	 * @return result with tag
	 */
	public static DataStream<Tuple3<String, Integer, String>> getWordCount(DataStream<TweetClass> tweets){
		return tweets
				.flatMap(new WordCount())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple3<String, Integer, String> longLongIntegerTuple3) {
						return System.currentTimeMillis();
					}
				})
				.keyBy(0,2)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
				.sum(1);
	}

	/**
	 * Writes th Data to Elastic Search
	 *
	 * @param tweets Datastream of TweetClasses
	 * @param httpHosts List of Address to ES
	 */
	private static void wordCount(DataStream<TweetClass> tweets, List<HttpHost> httpHosts){
		DataStream<Tuple3<String, Integer, String>> wordCount = getWordCount(tweets);

		ElasticsearchSink.Builder<Tuple3<String, Integer, String>> esWordCountSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Tuple3<String, Integer, String>>() {
					public IndexRequest createIndexRequest(Tuple3<String, Integer, String> element) {

						Map json = new HashMap<>();
						json.put("word", element.f0);
						json.put("count", element.f1);
						json.put("word_count_tag", element.f2);
						json.put("word_count_timestamp", System.currentTimeMillis());

						return Requests.indexRequest()
								.index("tw_word_count")
								.type("word_count")
								.source(json);
					}
					@Override
					public void process(Tuple3<String, Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		esWordCountSinkBuilder.setBulkFlushMaxActions(1);
		wordCount.addSink(esWordCountSinkBuilder.build());
	}

	/**
	 * Does Keybuckets of 10 min for the local storage sink
	 */
	public static final class KeyBucketAssigner implements BucketAssigner<TweetClass, String> {

		private static final long serialVersionUID = 987325769970523326L;

		@Override
		public String getBucketId(final TweetClass element, final Context context) {
			return String.valueOf(element.createdAt / (1000*60*10) * (1000*60*10));
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * Writes Data to local storage sink
	 *
	 * @param tweets Datastream of TweetClasses
	 */
	private static void writeDataToLocalSink(DataStream<TweetClass> tweets){
		final String outputPath = "/home/yannik/IdeaProjects/big_data/sink_data/";
		final StreamingFileSink<TweetClass> sink = StreamingFileSink
				.forRowFormat(new Path(outputPath), (Encoder<TweetClass>) (element, stream) -> {
					PrintStream out = new PrintStream(stream);
					Gson gson = new Gson();
					String json = gson.toJson(element);
					out.println(json);
				})
				.withBucketAssigner(new KeyBucketAssigner())
				.withRollingPolicy(OnCheckpointRollingPolicy.build())
				.build();

		tweets.addSink(sink);
	}


	/**
	 * Aggregation to get the Average and the Tag
	 */
	public static class AverageAggregateTag implements AggregateFunction<Tuple2<Integer, String>, Tuple3<Long, Long, String>, Tuple2<Double, String>> {
		@Override
		public Tuple3<Long, Long, String> createAccumulator() {
			return new Tuple3<>(0L, 0L, "");
		}

		@Override
		public Tuple3<Long, Long, String> add(Tuple2<Integer, String> newInput, Tuple3<Long, Long, String> accumulator) {
			if (!newInput.f1.equals(accumulator.f2) && !accumulator.f2.equals("")) System.out.println("ERROR ACC");
			return new Tuple3<>(accumulator.f0 + newInput.f0, accumulator.f1 + 1L, newInput.f1);
		}

		@Override
		public Tuple2<Double, String> getResult(Tuple3<Long, Long, String> accumulator) {
			return new Tuple2<>(((double) accumulator.f0 / accumulator.f1), accumulator.f2);
		}

		@Override
		public Tuple3<Long, Long, String> merge(Tuple3<Long, Long, String> acc, Tuple3<Long, Long, String> acc1) {
			return new Tuple3<>(acc.f0 + acc1.f0, acc.f1 + acc1.f1, acc.f2);
		}
	}
}
