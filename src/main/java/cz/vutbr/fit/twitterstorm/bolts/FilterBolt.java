package cz.vutbr.fit.twitterstorm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import cz.vutbr.fit.twitterstorm.util.Tweet;
import edu.stanford.nlp.pipeline.Annotation;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * A bolt checking whether tweets mention a computer game
 * Accepts: Tweets
 * Emits: Filtered list of tweets, list of Annotation objects for username and tweet content processing
 * @author ikouril
 */
public class FilterBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7563936820404254233L;
	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(FilterBolt.class);
	private Pattern pattern;
	
	String hostname;
	private Monitoring monitor;
	
	/**
     * Creates a new FilterBolt.
     * @param id the id of actual twitterstorm run
     */
	public FilterBolt(String id){
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
		pattern=Pattern.compile(TwitterStormTopology.pattern);
		try{
			hostname=InetAddress.getLocalHost().getHostName();
		}
		catch(UnknownHostException e){
			hostname="-unknown-";
		}
		log.info("Starting to filter tweets with following regular expression: "+pattern.pattern());
	}

	@Override
	public void execute(Tuple input) {
		long startTime = System.nanoTime();
		log.info("Entering filter bolt");
		String id=(String) input.getValue(0);
		ArrayList<Tweet> block=(ArrayList<Tweet>) input.getValue(1);
		//Tweet tweet=(Tweet) input.getValue(0);
		ArrayList<Tweet> tweets=new ArrayList<Tweet>();
		ArrayList<Annotation> authorAnnots=new ArrayList<Annotation>();
		ArrayList<Annotation> textAnnots=new ArrayList<Annotation>();
		for (Tweet tweet:block){
		
			String text=tweet.getText();
			Matcher m=pattern.matcher(text);
	    	StringBuilder output=new StringBuilder();
	    	Set<String> games=new HashSet<String>();
	        while (m.find()) {
	            games.add(m.group(1));
	        }
	        
	    	if (games.size()>0){
	    		
	    		
	    		Annotation textAnnot=new Annotation(text);
	    		String author=tweet.getAuthor();
	    		Annotation authorAnnot=new Annotation(author);
	    		
	    		authorAnnots.add(authorAnnot);
	    		textAnnots.add(textAnnot);
	    		
	    		for (String game:games){
	            	if (output.length()!=0)
	            		output.append(", ");
	                output.append(game);
	            }
	    		tweet.setGame(output.toString());
	    		tweets.add(tweet);
	    		
	    	}
		}
		if (tweets.size()>0){
			
			Long estimatedTime = System.nanoTime() - startTime;
			try {
				monitor.MonitorTuple("FilterBolt", id, tweets.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			log.info("Emmiting data in tweets for further processing");
			collector.emit("process",new Values(id,authorAnnots,textAnnots));
			
			log.info("Emmiting filtered tweets");
			collector.emit("filtered",new Values(id,tweets));
		}
		
		
    	collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("filtered",new Fields("id","filtered_tweet"));
		declarer.declareStream("process",new Fields("id","author_annot","text_annot"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

