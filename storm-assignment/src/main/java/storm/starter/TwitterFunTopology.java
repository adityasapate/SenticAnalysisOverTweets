package storm.starter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.bolt.FileWriterBolt;
import storm.starter.bolt.HashtagExtractionBolt;
import storm.starter.bolt.HyperLinkExtractBolt;
import storm.starter.bolt.LanguageDetectionBolt;
import storm.starter.bolt.PrinterBolt;
import storm.starter.bolt.SentimentAnalysisBolt;
import storm.starter.bolt.UserHashtagJoinBolt;
import storm.starter.bolt.UserSplitterBolt;
import storm.starter.spout.TwitterSpout;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.api.HelpResources.Language;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterFunTopology {

	private static String consumerKey = "xlQPZXQjc78hqHLmjzaATO30j";
	private static String consumerSecret = "4oBQSkIM62bTvgpQqSxNwRKJCYMbghr5Zvg6AhzQ13MCwKSCbG";
	private static String accessToken = "499663420-UPx1f5cWTb2rJ5fyD0IjKmuePVMGMUNKVYyEostv";
	private static String accessTokenSecret = "IhelPU92SGAJOevFH5pWLVANEIDCn4gIjyr1UwXA9GYq3";

	public static void main(String[] args) throws Exception {
		/**************** SETUP ****************/
		String remoteClusterTopologyName = null;
		if (args != null) {
			if (args.length == 1) {
				remoteClusterTopologyName = args[0];
			}
			// If credentials are provided as commandline arguments
			else if (args.length == 4) {
				consumerKey = args[0];
				consumerSecret = args[1];
				accessToken = args[2];
				accessTokenSecret = args[3];
			}

		}
		/****************       ****************/

		TopologyBuilder builder = new TopologyBuilder();

		FilterQuery tweetFilterQuery = new FilterQuery();

		tweetFilterQuery.locations(new double[][] { new double[] { -180, -90 },
				new double[] { 180, 90 } });

		tweetFilterQuery.language(new String[] { "en" });

		builder.setSpout("spout", new TwitterSpout(consumerKey, consumerSecret,
				accessToken, accessTokenSecret, tweetFilterQuery), 1);
		
		builder.setBolt("hyperlink", new HyperLinkExtractBolt(),1 ).shuffleGrouping("spout");
		
		builder.setBolt("hashtag", new HashtagExtractionBolt()).shuffleGrouping("hyperlink");
		
		builder.setBolt("attherate", new UserSplitterBolt()).shuffleGrouping("hashtag");
		
		builder.setBolt("sentiment", new SentimentAnalysisBolt()).shuffleGrouping("attherate");
		
		builder.setBolt("file-print", new FileWriterBolt("/home/aditya/tweets.txt")).shuffleGrouping("sentiment");

//		builder.setBolt("user-spitter", new UserSplitterBolt(), 4)
//				.shuffleGrouping("spout");
//		builder.setBolt("hashtag-splitter", new HashtagExtractionBolt(), 4)
//				.shuffleGrouping("spout");
//
//		builder.setBolt("user-hashtag-merger", new UserHashtagJoinBolt(), 4)
//				.fieldsGrouping("user-spitter", "user", new Fields("tweet_id"))
//				.fieldsGrouping("hashtag-spitter", "hashtags", new Fields("tweet_id"));
//
//		builder.setBolt("file-writer",
//				new FileWriterBolt("/home/aditya/tweetsenti.txt"), 1)
//				.globalGrouping("user-spitter", "users")
//				.globalGrouping("hashtag-spitter", "hashtags")
//				.globalGrouping("user-hashtag-merger");

		Config conf = new Config();

		conf.setDebug(false);

		if (remoteClusterTopologyName != null) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(remoteClusterTopologyName, conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitter-fun", conf,
					builder.createTopology());

			Thread.sleep(460000);

			cluster.shutdown();
		}
	}
}