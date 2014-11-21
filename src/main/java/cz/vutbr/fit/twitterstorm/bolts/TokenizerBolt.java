package cz.vutbr.fit.twitterstorm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
* A bolt for tokenizing input texts
* Accepts: Annotation objects
* Emits: Annotations objects with tokens information
* @author ikouril
*/
public class TokenizerBolt implements IRichBolt {

	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(TokenizerBolt.class);
	private Annotator tokenizer;
	
	String hostname;
	private Monitoring monitor;
	
	/**
	 * Creates a new TokenizerBolt.
	 * @param id the id of actual twitterstorm run
	 */
	public TokenizerBolt(String id){
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
		tokenizer=TwitterStormTopology.pipeline.getExistingAnnotator("tokenize");
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
		log.info("Entering tokenizer bolt");
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(2);
		ArrayList<Annotation> authorAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		
		for (int i=0;i<authorAnnots.size();i++){
			tokenizer.annotate(authorAnnots.get(i));
			tokenizer.annotate(textAnnots.get(i));
		}
		
		log.info("Sending tokenized data");
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("ParseBolt", id, textAnnots.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("tokenized",new Values(id,authorAnnots,textAnnots));
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tokenized",new Fields("id","author_tokens","text_tokens"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

