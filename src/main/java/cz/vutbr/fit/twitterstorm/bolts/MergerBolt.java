package cz.vutbr.fit.twitterstorm.bolts;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import cz.vutbr.fit.twitterstorm.util.Tweet;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
/**
 * Bolt for merging IndexBolts' results of a query
 * Accepts: Tweets matching given query
 * @author ikouril
 */
public class MergerBolt implements IRichBolt {

	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(MergerBolt.class);
	private HashMap<String,ArrayList<ArrayList<Tweet>>> queryResults=new HashMap<String,ArrayList<ArrayList<Tweet>>>();
	private ConnectionFactory factory;
	private Connection connection;
	Channel channel;
	
	class TweetComparator implements Comparator<Tweet>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8309113527026653000L;

		@Override
		public int compare(Tweet t1, Tweet t2)
        {
        	Float score1=t1.getScore();
        	Float score2=t2.getScore();
        	return score2.compareTo(score1);
        } 
	}
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		try{
			factory = new ConnectionFactory();
		    connection = factory.newConnection("localhost");
		    channel = connection.createChannel();
		    channel.queueDeclare(1,"result");
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple input) {
		log.info("Entering merger bolt");
		ArrayList<Tweet> data=(ArrayList<Tweet>) input.getValue(0);
		String key=(String) input.getValue(1);
		
		ArrayList<ArrayList<Tweet>> results=queryResults.get(key);
		if (results==null){
			results=new ArrayList<ArrayList<Tweet>>();
			results.add(data);
			queryResults.put(key, results);
		}
		else{
			results.add(data);
			if (results.size()==TwitterStormTopology.PARALLELISM){
				ArrayList<Tweet> responses=results.get(0);
				for (int i=1;i<TwitterStormTopology.PARALLELISM;i++){
					responses.addAll(results.get(i));
				}
				Collections.sort(responses, new TweetComparator());
				StringBuilder res=new StringBuilder();
				res.append("Results: "+String.valueOf(responses.size())+"\n");
				int order=0;
				for (Tweet oneRes:responses){
					res.append(String.valueOf(++order)+".\n");
					res.append("Score: "+String.valueOf(oneRes.getScore())+"\n");
					res.append("Author: "+oneRes.getAuthor()+"\n");
					res.append("Date: "+oneRes.getDate().toString()+"\n");
					res.append("Person: "+oneRes.getPerson()+"\n");
					res.append("Game: "+oneRes.getGame()+"\n");
					res.append("Text: "+oneRes.getText()+"\n");
					res.append("Sentiment: "+oneRes.getSentiment()+"\n");
					res.append("Gender: "+oneRes.getGender()+"\n");
				}
				try {
					log.info("Publishing query results");
					channel.basicPublish(1, "","result", null, res.toString().getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}
				queryResults.remove(key);
			}	
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
