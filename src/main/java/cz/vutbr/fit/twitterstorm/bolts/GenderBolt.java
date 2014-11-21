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
import edu.stanford.nlp.ie.machinereading.structure.MachineReadingAnnotations.GenderAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations;
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
 * A bolt classifying gender of tweets' authors
 * Accepts: list of author name Annotations
 * Emits: Gender of author
 * @author ikouril
 */
public class GenderBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2731668299625166265L;
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(GenderBolt.class);
	private Annotator genderClassifier;
	
	String hostname;
	private Monitoring monitor;
	
	/**
     * Creates a new GenderBolt.
     * @param id the id of actual twitterstorm run
     */
	public GenderBolt(String id){
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
		genderClassifier=TwitterStormTopology.pipeline.getExistingAnnotator("gender");
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
		log.info("Entering gender bolt");

		ArrayList<Annotation> authorAnnots=(ArrayList<Annotation>) input.getValue(1);
		String id=(String) input.getValue(0);
		ArrayList<String> genders=new ArrayList<String>();
		for (int i=0;i<authorAnnots.size();i++){
			genderClassifier.annotate(authorAnnots.get(i));
			boolean genderResolved=false;
			String gender="unresolved";
			List<CoreMap> sentences = authorAnnots.get(i).get(CoreAnnotations.SentencesAnnotation.class);
			for (CoreMap sentence : sentences) {
			  for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				  String result = token.get(GenderAnnotation.class);
				  if (result!=null){
					  gender=result.toLowerCase();
					  genderResolved=true;
					  break;
				  }
	            }
			  if (genderResolved)
				  break;
			}
			genders.add(gender);
		}
		log.info("Sending gender");
		Long estimatedTime = System.nanoTime() - startTime;
		try {
			monitor.MonitorTuple("GenderBolt", id, genders.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	collector.emit("gendered",new Values(id,genders));
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("gendered",new Fields("id","gender"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
