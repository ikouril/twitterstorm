package cz.vutbr.fit.twitterstorm.spouts;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
* A spout for emitting query to internal index
* Emits: query string
* @author ikouril
*/
public class QuerySpout extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3171275129739579120L;
	private SpoutOutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(QuerySpout.class);
	private QueueingConsumer consumer;
	private Channel channel;
	private ConnectionFactory factory;
	private Connection connection;
	private QueueingConsumer.Delivery delivery;
	
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector=collector;
		
		factory = new ConnectionFactory();
		try {
			connection = factory.newConnection("localhost");
			channel = connection.createChannel();
		    channel.queueDeclare(1,"query");
		    consumer = new QueueingConsumer(channel);
		    channel.basicConsume(1, "query", true, consumer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public void nextTuple() {
		try {
			delivery = consumer.nextDelivery();
			String message = new String(delivery.getBody());
			log.info("Query to process: "+message);
			collector.emit("query",new Values(message,UUID.randomUUID().toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}

	    

	}	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("query", new Fields("query_string","query_id"));
		
	}
}

