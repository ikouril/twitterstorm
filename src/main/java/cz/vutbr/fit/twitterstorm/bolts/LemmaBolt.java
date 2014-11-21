package cz.vutbr.fit.twitterstorm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.Annotator;
import edu.stanford.nlp.util.CoreMap;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * Bolt for lemmatizing tweet text and extracting keywords
 * Accepts: Tweet text annotations
 * Emits: Multiset of keywords
 * @author ikouril
 */
public class LemmaBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2035866012100889743L;
	/**
	 * 
	 */
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(LemmaBolt.class);
	private Annotator lemmatizer;

	String hostname;
	private Monitoring monitor;
	
	/**
     * Creates a new LemmaBolt.
     * @param uuid the id of actual twitterstorm run
     */
	public LemmaBolt(String id){
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
		lemmatizer=TwitterStormTopology.pipeline.getExistingAnnotator("lemma");
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
		log.info("Entering lemma bolt");
		
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		ArrayList<Multiset<String>> allKeywords=new ArrayList<Multiset<String>>();
		for (int i=0;i<textAnnots.size();i++){
			lemmatizer.annotate(textAnnots.get(i));

			Multiset<String> keywords=HashMultiset.create();
			List<CoreMap> sentences = textAnnots.get(i).get(CoreAnnotations.SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
			  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				  String tag=token.get(PartOfSpeechAnnotation.class);
				  if (tag.equals("FW") || tag.startsWith("VB") || tag.startsWith("NN")){
					  String lemma=token.get(LemmaAnnotation.class);
					  if (lemma.length()>2)
						  keywords.add(lemma);
				  }	
	            }
			}
			allKeywords.add(keywords);
		}
		
		log.info("Sending keywords");
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("LemmaBolt", id, textAnnots.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("keywords",new Values(id,allKeywords));
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("keywords",new Fields("id","keyword"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}