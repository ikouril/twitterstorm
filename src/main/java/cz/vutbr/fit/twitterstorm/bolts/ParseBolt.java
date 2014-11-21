package cz.vutbr.fit.twitterstorm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
/**
 * A bolt for parsing tweet texts
 * Accepts: Tweet text annotations
 * Emits: List of Annotation objects for tweet content
 * @author ikouril
 */
public class ParseBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2731668299625166265L;
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(ParseBolt.class);
	private Annotator parser;
	
	String hostname;
	private Monitoring monitor;
	
	/**
	 * Creates a new ParseBolt.
	 * @param id the id of actual twitterstorm run
	 */
	public ParseBolt(String id){
		try {
			monitor=new Monitoring(id, "knot28.fit.vutbr.cz", "twitterstorm", "twitterstormdb88pass", "twitterstormdb");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		parser=TwitterStormTopology.pipeline.getExistingAnnotator("parse");
		try{
			hostname=InetAddress.getLocalHost().getHostName();
		}
		catch(UnknownHostException e){
			hostname="-unknown-";
		}
	}

	@Override
	public void execute(Tuple input) {
		long startTime = System.nanoTime();
		log.info("Entering parser bolt");
		
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		for (int i=0;i<textAnnots.size();i++){
			parser.annotate(textAnnots.get(i));
		}
		
		
		log.info("Sending parsed data");
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("ParseBolt", id, textAnnots.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("parsed",new Values(id,textAnnots));
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("parsed",new Fields("id","parses"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
