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
* A bolt for sentence splitting
* Accepts: Annotation objects
* Emits: Annotation objects with sentence splits information
* @author ikouril
*/
public class SentenceSplitterBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2731668299625166265L;
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(SentenceSplitterBolt.class);
	private Annotator sentenceSplitter;
	
	String hostname;
	private Monitoring monitor;
	
	/**
	 * Creates a new SentenceSplitterBolt.
	 * @param id the id of actual twitterstorm run
	 */
	public SentenceSplitterBolt(String id){
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
		sentenceSplitter=TwitterStormTopology.pipeline.getExistingAnnotator("ssplit");
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
		log.info("Entering sentenceSplitter bolt");
		
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(2);
		ArrayList<Annotation> authorAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		
		for (int i=0;i<textAnnots.size();i++){
			sentenceSplitter.annotate(textAnnots.get(i));
			sentenceSplitter.annotate(authorAnnots.get(i));
		}
		
		log.info("Sending sentenced tweets");
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("ParseBolt", id, textAnnots.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("sentenced",new Values(id,authorAnnots,textAnnots));
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("sentenced",new Fields("id","author_sentences","text_sentences"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
