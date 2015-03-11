package cz.vutbr.fit.twitterstorm.spouts;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import cz.vutbr.fit.twitterstorm.util.Tweet;

import org.apache.commons.lang.StringUtils;

/**
* A spout for emitting tweets read directly from twitter streaming API
* Emits: List of tweets
* @author ikouril
*/
public class TwitterSpout extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3171275129739579120L;
	private SpoutOutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(TwitterSpout.class);
	private static final String CONSUMER_KEY="put your customer key here";
	private static final String CONSUMER_KEY_SECRET="put your customer key secret here";
	private static final String TOKEN="put your token here";
	private static final String TOKEN_SECRET="put your token secret here";
	private StatusListener listener;
	private TwitterStream twitterStream;
	private AccessToken access;
	private LinkedList<Tweet> inputs;
	private ArrayList<Tweet> block;
	private int blockSize;
	String hostname;
	private Monitoring monitor;
	
	/**
	 * Creates a new TwitterSpout.
	 * @param id the id of actual twitterstorm run
	 */
	public TwitterSpout(int blockSize,String id){
		this.blockSize=blockSize;
		try {
			monitor=new Monitoring(id, "knot28.fit.vutbr.cz", "twitterstorm", "twitterstormdb88pass", "twitterstormdb");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector=collector;
		inputs=new LinkedList<Tweet>();
		block=new ArrayList<Tweet>();
		listener = new StatusListener(){
	        public void onStatus(Status status) {
	        	log.info("New tweet arrived");
	        	inputs.add(new Tweet(status.getText(),status.getUser().getName(),status.getCreatedAt()));
	        	
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			public void onScrubGeo(long arg0, long arg1) {
			}
			public void onStallWarning(StallWarning arg0) {
			}
	    };
		
	    twitterStream = new TwitterStreamFactory().getInstance();
	    twitterStream.setOAuthConsumer(CONSUMER_KEY, CONSUMER_KEY_SECRET);
	    access = new AccessToken(TOKEN,TOKEN_SECRET);
	    twitterStream.setOAuthAccessToken(access);

	    twitterStream.addListener(listener);
	    FilterQuery filtre = new FilterQuery();
	    String[] languageArray = { "en" };
	    log.info("Starting to listen for tweets with keywords: "+StringUtils.join(TwitterStormTopology.keywords,"\n"));
	    filtre.track(TwitterStormTopology.keywords);
	    filtre.language(languageArray);
	    twitterStream.filter(filtre);

	}
	

	@Override
	public void nextTuple() {
		long startTime = System.nanoTime();
		log.info("Entering twitter spout");
		if (inputs.size()>0){
			Tweet tweet=inputs.poll();
			if (block.size()==blockSize){
				log.info("Emitting tweets");
				Long estimatedTime = System.nanoTime() - startTime;
				String id=UUID.randomUUID().toString();
				final ArrayList<Tweet> blockToSent=new ArrayList<Tweet>(block);
				try {
					monitor.MonitorTuple("TwitterSpout", id, block.size(),hostname, estimatedTime);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				collector.emit("twitter", new Values(id,blockToSent));
				block.clear();
			}
			else{
				block.add(tweet);
			}
		}
	}	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("twitter", new Fields("id","tweet"));
		
	}
}
