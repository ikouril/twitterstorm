package cz.vutbr.fit.twitterstorm.spouts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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

import org.apache.lucene.document.DateTools;

/**
* A spout for emitting tweets read from dumps
* Emits: List of tweets
* @author ikouril
*/
public class DumpSpout extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3171275129739579120L;
	private SpoutOutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(DumpSpout.class);
	private final ArrayList<Tweet> block=new ArrayList<Tweet>();
	private int blockSize;
	private int spoutId;
	BufferedReader reader;
	private static int spoutCounter=0;
	String hostname;
	private Monitoring monitor;
	
	/**
	 * Creates a new DumpSpout.
	 * @param id the id of actual twitterstorm run
	 */
	public DumpSpout(int blockSize,String id){
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
		spoutId=spoutCounter;
		spoutCounter+=1;
		log.info("Dump spout id: "+String.valueOf(spoutId));
		try {
			reader=new BufferedReader(new InputStreamReader(new URL("http://"+TwitterStormTopology.FILES[spoutId]).openStream()));
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try{
			hostname=InetAddress.getLocalHost().getHostName();
		}
		catch(UnknownHostException e){
			hostname="-unknown-";
		}
	}
	

	@Override
	public void nextTuple() {
		long startTime = System.nanoTime();
		log.info("Entering dump spout");
		String line = null;
		try {
			line = reader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (line!=null){
			JSONObject json = null;
			json = (JSONObject) JSONValue.parse(line);
			String tweetDate=(String) json.get("date");
			Date date = null;
			try {
				date = tweetDate==null?null:DateTools.stringToDate(tweetDate);
			} catch (java.text.ParseException e) {
				date=null;
			}
			Tweet tweet=new Tweet((String)json.get("text"), (String)json.get("name"), date);
			if (block.size()==blockSize){
				log.info("Emmitting tweets");
				final ArrayList<Tweet> blockToSent=new ArrayList<Tweet>(block);
				Long estimatedTime = System.nanoTime() - startTime;
				String id=UUID.randomUUID().toString();
				try {
					monitor.MonitorTuple("DumpSpout", id, block.size(),hostname, estimatedTime);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				collector.emit("dump", new Values(id,blockToSent));
				block.clear();
			}
			else{
				block.add(tweet);
			}
		}
	}	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("dump", new Fields("id","tweet"));
		
	}
}