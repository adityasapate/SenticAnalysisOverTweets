package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

public class UserHashtagJoinBolt extends BaseRichBolt {
    private OutputCollector _collector;
    HashMap<String, String> tweetHashtags = new HashMap<String, String>(); 
    HashMap<String, String> userTweets = new HashMap<String, String>();


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
    	String source = tuple.getSourceStreamId();
    	String tweetId = tuple.getStringByField("tweet_id");
    	if("hashtags".equals(source)) {
    	String hashtag = tuple.getStringByField("hashtag");
    	tweetHashtags.put(tweetId, hashtag);
    	} else if("users".equals(source)) {
    	String user = tuple.getStringByField("user");
    	userTweets.put(user, tweetId);
    	}
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("entity"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
