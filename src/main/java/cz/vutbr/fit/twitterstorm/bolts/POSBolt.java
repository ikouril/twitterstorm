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
* A bolt for Part-of-speech tagging
* Accepts: Annotation objects
* Emits: Annotation objects with POS information
* @author ikouril
*/
public class POSBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2731668299625166265L;
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(POSBolt.class);
	private Annotator posTagger;
	
	String hostname;
	private Monitoring monitor;
	
	/**
	 * Creates a new POSBolt.
	 * @param id the id of actual twitterstorm run
	 */
	public POSBolt(String id){
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
		posTagger=TwitterStormTopology.pipeline.getExistingAnnotator("pos");
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
		log.info("Entering POS bolt");
		
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(2);
		ArrayList<Annotation> authorAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		
		for (int i=0;i<textAnnots.size();i++){
			posTagger.annotate(textAnnots.get(i));
			posTagger.annotate(authorAnnots.get(i));
		}
		
		
		log.info("Sending POS analyzed data");
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("POSBolt", id, textAnnots.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("to_gender",new Values(id,authorAnnots));
    	collector.emit("to_ner",new Values(id,textAnnots));
    	collector.emit("to_keywords",new Values(id,textAnnots));
    	collector.emit("to_sentiment",new Values(id,textAnnots));
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("to_gender",new Fields("id","author_pos"));
		declarer.declareStream("to_ner",new Fields("id","author_pos"));
		declarer.declareStream("to_keywords",new Fields("id","author_pos"));
		declarer.declareStream("to_sentiment",new Fields("id","author_pos"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
