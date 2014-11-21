package cz.vutbr.fit.twitterstorm.bolts;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
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
 * Bolt for extracting persons' names from tweets
 * Accepts: Tweet text annotations
 * Emits: List of persons' names
 * @author ikouril
 */
public class NERBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2731668299625166265L;
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(NERBolt.class);
	private Annotator nerTagger;
	
	String hostname;
	private Monitoring monitor;
	
	/**
     * Creates a new NERBolt.
     * @param id the id of actual twitterstorm run
     */
	public NERBolt(String id){
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
		nerTagger=TwitterStormTopology.pipeline.getExistingAnnotator("ner");
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
		log.info("Entering ner bolt");
		
		ArrayList<Annotation> textAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		ArrayList<String> allPersonsList=new ArrayList<String>();
		
		for (int i=0;i<textAnnots.size();i++){
			nerTagger.annotate(textAnnots.get(i));
			
			String actualAnnot="";
			String actualPerson="";
			String allPersons="";
			
			List<CoreMap> sentences = textAnnots.get(i).get(CoreAnnotations.SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
			  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				  actualAnnot=token.get(NamedEntityTagAnnotation.class);
				  if (actualAnnot.equals("PERSON")){
					  String actualToken=token.originalText();
					  if (!actualPerson.isEmpty())
						  actualPerson+=" "+actualToken;
					  else
						  actualPerson=actualToken;
				  }
				  else{
					  if (!actualPerson.isEmpty()){
						  if (!allPersons.isEmpty())
							  allPersons+=", "+actualPerson;
						  else
							  allPersons=actualPerson;
						  actualPerson="";
					  }
				  }
	            }
			}
			
			if (actualAnnot.equals("PERSON")){
				if (!allPersons.isEmpty())
					allPersons+=", "+actualPerson;
				else
					allPersons=actualPerson;
			}
			allPersonsList.add(allPersons);
		}
		log.info("Sending persons");
		
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("NERBolt", id, textAnnots.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("persons",new Values(id,allPersonsList));
		
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("persons",new Fields("id","person"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}