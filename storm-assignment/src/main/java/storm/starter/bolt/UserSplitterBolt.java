package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

public class UserSplitterBolt extends BaseRichBolt {
    private OutputCollector _collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
    	
    	String message = tuple.getStringByField("message");
    	String tweet = tuple.getStringByField("trim");
    	String tweetId = tuple.getStringByField("tweet_id");
    	StringTokenizer strTok = new StringTokenizer(tweet, " ");
    	StringBuilder str = new StringBuilder();
    	
    	while(strTok.hasMoreTokens()) {
	    	String word = (String) strTok.nextElement();
	    	if(word.startsWith("@") ) {
	    			str.append("Adam");
	    	}else{
	    		str.append(word);
	    	}
	    	str.append(" ");
    	}
    	
    	
    	_collector.emit(new Values(tweetId, message, str.toString()));

    	        // Confirm that this tuple has been treated.
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet_id","message", "trim"));

    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
