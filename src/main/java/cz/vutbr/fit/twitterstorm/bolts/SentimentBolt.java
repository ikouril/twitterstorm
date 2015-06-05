package cz.vutbr.fit.twitterstorm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
/**
* A bolt for sentiment analysis
* Accepts: Annotation objects
* Emits: Extracted sentiments of given texts
* @author ikouril
*/
public class SentimentBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2731668299625166265L;
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(SentimentBolt.class);
	private Annotator sentimentClassifier;
	
	String hostname;
	private Monitoring monitor;
	
	/**
	 * Creates a new SentimentBolt.
	 * @param id the id of actual twitterstorm run
	 */
	public SentimentBolt(String id){
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
		sentimentClassifier=TwitterStormTopology.pipeline.getExistingAnnotator("sentiment");
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
		log.info("Entering sentiment bolt");
		
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		ArrayList<String> sentiments=new ArrayList<String>();
		for (int i=0;i<textAnnots.size();i++){
			sentimentClassifier.annotate(textAnnots.get(i));
			
			List<CoreMap> sentences = textAnnots.get(i).get(CoreAnnotations.SentencesAnnotation.class);
			int positive=0;
			int negative=0;
			String sentiment="Neutral";
			for (CoreMap sentence : sentences) {
				String result = sentence.get(SentimentCoreAnnotations.ClassName.class);
				if (result.equals("Positive"))
					positive++;
				else if (result.equals("Negative"))
					negative++;
			}
			
			if (positive>negative)
				sentiment="Positive";
			else if (negative>positive)
				sentiment="Negative";
			sentiments.add(sentiment);
		}
		log.info("Sending sentimented data");
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("SentimentBolt", id, textAnnots.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("sentimented",new Values(id,sentiments));
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("sentimented",new Fields("id","sentiment"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}