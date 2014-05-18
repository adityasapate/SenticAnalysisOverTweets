package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.StringTokenizer;

public class HyperLinkExtractBolt extends BaseRichBolt {
    private OutputCollector _collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        String text = tuple.getStringByField("message");
        String tweet_id = tuple.getStringByField("tweet_id");
        StringTokenizer st = new StringTokenizer(text);
        
        StringBuilder str = new StringBuilder();
        
        System.out.println("---- Split by space ------");
        while (st.hasMoreElements()) {
        	String word = (String) st.nextElement();
        	
        	try {
                URL url = new URL(word);
                
            } catch (MalformedURLException e) {
                // TODO Auto-generated catch block
                    str.append(word);
                    str.append(" ");
            }   


        }
        
        

        _collector.emit(new Values(tweet_id, text, str.toString()));
        // Confirm that this tuple has been treated.
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet_id", "message", "trim"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
