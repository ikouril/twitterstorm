package cz.vutbr.fit.twitterstorm.bolts;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.twitterstorm.topologies.TwitterStormTopology;
import cz.vutbr.fit.twitterstorm.util.IndexingStrategy;
import cz.vutbr.fit.twitterstorm.util.InfoHolder;
import cz.vutbr.fit.twitterstorm.util.Tweet;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bolt for merging pipeline processing results, which are indexed and can be queried, outputs statistics via rabbitMQ
 * Accepts: Tweets, Genders, Keywords, Sentiments, Persons, Query
 * Emits: Only Query results to be filtered
 * @author ikouril
 */
public class IndexBolt implements IRichBolt {

	private OutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(IndexBolt.class);
	IndexWriterConfig conf=null;
    Directory directory=null;
    IndexWriter iw=null;
    IndexReader ir=null;
    Analyzer analyzer=null;
    Map<String,ArrayList<InfoHolder>> infos=new HashMap<String,ArrayList<InfoHolder>>();
    Multiset<String> positiveKeywords;
    Multiset<String> negativeKeywords;
    Multiset<String> allKeywords;
    private ConnectionFactory factory;
	private Connection connection;
	Channel channel;
	BulkProcessor bulkProcessor;

	IndexingStrategy strategy;
	private boolean ram;
	String hostname;
	private Monitoring monitor;
	
	private static Integer instances=0;
	
	
	
	/**
     * Creates a new IndexBolt.
     * @param id the id of actual twitterstorm run
     * @param strategy the actual indexing strategy, which can be internal (lucene), external (elasticsearch) or both
     * @param ram
     */
	public IndexBolt(String id,IndexingStrategy strategy,boolean ram){
		try {
			monitor=new Monitoring(id, "knot28.fit.vutbr.cz", "twitterstorm", "twitterstormdb88pass", "twitterstormdb");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.strategy=strategy;
		this.ram=ram;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		int nodeId;
		synchronized(instances){
			nodeId=++instances;
		}
		if (analyzer==null)
			analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		if (conf==null)
			conf = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
		if (directory==null){
			if (ram)
				directory = new RAMDirectory();
			else{
				try {
					String path=System.getProperty("user.home")+"/twitterindex/"+String.valueOf(nodeId)+"/";
					File f=new File(path);
					f.mkdirs();
					directory=FSDirectory.open(f);
				} catch (IOException e) {
					directory=new RAMDirectory();
					e.printStackTrace();
				}
			}
		}
		if (iw==null){
	        try {
				iw = new IndexWriter(directory, conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			iw.commit();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try{
			factory = new ConnectionFactory();
		    connection = factory.newConnection("localhost");
		    channel = connection.createChannel();
		    channel.queueDeclare(1,"result");
		}
		catch (Exception e){
			e.printStackTrace();
		}
		allKeywords=HashMultiset.create();
		positiveKeywords=HashMultiset.create();
		negativeKeywords=HashMultiset.create();
		
		bulkProcessor = BulkProcessor.builder(
		        TwitterStormTopology.client,  new BulkProcessor.Listener() {

					public void afterBulk(long arg0, BulkRequest arg1,
							BulkResponse arg2) {
						// TODO Auto-generated method stub
						
					}

					public void afterBulk(long arg0, BulkRequest arg1,
							Throwable arg2) {
						arg2.printStackTrace();
						
					}

					public void beforeBulk(long arg0, BulkRequest arg1) {
						// TODO Auto-generated method stub
						
					}
		           })
		        .setFlushInterval(new TimeValue(5000))
		        .build();
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
		log.info("Entering index bolt");
		String streamID=input.getSourceStreamId();
		if (streamID.equals("query")){
			String queryString=(String) input.getValue(0);
			
			if (queryString.equals("stat")){
				log.info("Publishing statistics");
				StringBuilder res=new StringBuilder();
				res.append("Positive:\n=========\n\n");
				int i=1;
				for (String positive : Multisets.copyHighestCountFirst(positiveKeywords).elementSet()) {
				    res.append(String.valueOf(i)+". "+positive + ": " + positiveKeywords.count(positive)+"\n");
				    i++;
				    if (i>10)
				    	break;
				}
				res.append("\n");
				res.append("Negative:\n=========\n\n");
				i=1;
				for (String negative : Multisets.copyHighestCountFirst(negativeKeywords).elementSet()) {
				    res.append(String.valueOf(i)+". "+negative + ": " + negativeKeywords.count(negative)+"\n");
				    i++;
				    if (i>10)
				    	break;
				}
				res.append("\n");
				res.append("Overall:\n========\n\n");
				i=1;
				for (String overall : Multisets.copyHighestCountFirst(allKeywords).elementSet()) {
				    res.append(String.valueOf(i)+". "+overall + ": " + negativeKeywords.count(overall)+"\n");
				    i++;
				    if (i>10)
				    	break;
				}
				try {
					channel.basicPublish(1, "","result", null, res.toString().getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else{
				log.info("Got query: "+queryString);
				try {
					ir = IndexReader.open(directory);
					
					Query q = new MultiFieldQueryParser(Version.LUCENE_CURRENT, new String[]{"game","author","text","person","sentiment","gender","datadisc","variant","part"}, analyzer).parse(queryString);
					IndexSearcher searcher = new IndexSearcher(ir);
	
					TopDocs results=searcher.search(q, 10);
					ScoreDoc[] hits = results.scoreDocs;
					
					List<Tweet> tweets=new ArrayList<Tweet>();
					
					for (ScoreDoc hit:hits){
						Tweet t=new Tweet();
						int docId=hit.doc;
						Document d=searcher.doc(docId);
						t.setScore(hit.score);
						t.setPerson(d.get("person"));
						t.setGame(d.get("game"));
						String dateString=d.get("date");
						t.setDate(dateString.equals("null")?null:DateTools.stringToDate(dateString));
						t.setAuthor(d.get("author"));
						t.setText(d.get("text"));
						t.setSentiment(d.get("sentiment"));
						t.setGender(d.get("gender"));
						t.setDatadisc(d.get("datadisc"));
						t.setVariant(d.get("variant"));
						t.setPart(d.get("part"));
						tweets.add(t);
					}
					log.info("Sending "+String.valueOf(tweets.size())+" scored tweets");
					collector.emit("scored",new Values(tweets,input.getValue(1)));
				
	
					ir.close();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				} catch (java.text.ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else if (streamID.equals("filtered")){
			ArrayList<Tweet> tweets=(ArrayList<Tweet>) input.getValue(1);
			String id=(String) input.getValue(0);
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<tweets.size();i++){
					InfoHolder h=new InfoHolder();
					h.setTweet(tweets.get(i));
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setTweet(tweets.get(i));
				}
				checkHolders(holders,id);
			}
			Long estimatedTime = System.nanoTime() - startTime;
			try {
				monitor.MonitorTuple("IndexBolt", id, tweets.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (streamID.equals("gendered")){
			ArrayList<String> genders=(ArrayList<String>) input.getValue(1);
			String id=(String) input.getValue(0);
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<genders.size();i++){
					InfoHolder h=new InfoHolder();
					h.setGender(genders.get(i));
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setGender(genders.get(i));
				}
				checkHolders(holders,id);
			}
			Long estimatedTime = System.nanoTime() - startTime;
			try {
				monitor.MonitorTuple("IndexBolt", id, genders.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (streamID.equals("keywords")){
			ArrayList<Multiset<String>> allKeywords=(ArrayList<Multiset<String>>) input.getValue(1);
			String id=(String) input.getValue(0);
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<allKeywords.size();i++){
					InfoHolder h=new InfoHolder();
					h.setKeywords(allKeywords.get(i));
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setKeywords(allKeywords.get(i));
				}
				checkHolders(holders,id);
			}
			Long estimatedTime = System.nanoTime() - startTime;
			try {
				monitor.MonitorTuple("IndexBolt", id, allKeywords.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (streamID.equals("persons")){
			ArrayList<String> persons=(ArrayList<String>) input.getValue(1);
			String id=(String) input.getValue(0);
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<persons.size();i++){
					InfoHolder h=new InfoHolder();
					h.setPersons(persons.get(i));
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setPersons(persons.get(i));
				}
				checkHolders(holders,id);
			}
			Long estimatedTime = System.nanoTime() - startTime;
			try {
				monitor.MonitorTuple("IndexBolt", id, persons.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (streamID.equals("sentimented")){
			ArrayList<String> sentiments=(ArrayList<String>) input.getValue(1);
			String id=(String) input.getValue(0);
			ArrayList<InfoHolder> holders=infos.get(id);
			if (holders==null){
				holders=new ArrayList<InfoHolder>();
				for (int i=0;i<sentiments.size();i++){
					InfoHolder h=new InfoHolder();
					h.setSentiment(sentiments.get(i));
					holders.add(h);
				}
				infos.put(id, holders);
			}
			else{
				for (int i=0;i<holders.size();i++){
					holders.get(i).setSentiment(sentiments.get(i));
				}
				checkHolders(holders,id);
			}
			Long estimatedTime = System.nanoTime() - startTime;
			try {
				monitor.MonitorTuple("IndexBolt", id, sentiments.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
		
		collector.ack(input);
	}
	
	private boolean holdersComplete(ArrayList<InfoHolder> holders){
		for (int i=0;i<holders.size();i++){
			if (!holders.get(i).isComplete())
				return false;
		}
		return true;
	}

	private void checkHolders(ArrayList<InfoHolder> holders,String id) {
		if (holdersComplete(holders)){
			for (int i=0;i<holders.size();i++){
				Tweet tweet=holders.get(i).getResult();
				String game=tweet.getGame();
				String author=tweet.getAuthor();
				String text=tweet.getText();
				Date date=tweet.getDate();
				String datadisc=tweet.getDatadisc();
				String part=tweet.getPart();
				String variant=tweet.getVariant();
				String dateString=date==null?"null":DateTools.dateToString(date, DateTools.Resolution.SECOND);
				String person=tweet.getPerson();
				String sentiment=tweet.getSentiment();
				String gender=tweet.getGender();
				Multiset<String> originalKeywords=tweet.getKeywords();
				String keywords="";
				for (String keyword:originalKeywords.elementSet()){
					if (keywords.isEmpty())
						keywords=keyword;
					else
						keywords+=", "+keyword;
					int count=originalKeywords.count(keyword);
					if (sentiment.equals("Positive"))
						positiveKeywords.add(keyword, count);
					else if (sentiment.equals("Negative"))
						negativeKeywords.add(keyword, count);
					allKeywords.add(keyword, count);
				}
				
				
				if (strategy!=IndexingStrategy.EXTERNAL){
				
					log.info("Indexing tweet");
					Document document=new Document();
					document.add(new Field("game",game, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("datadisc",datadisc, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("part",part, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("variant",variant, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("author",author, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("text",text, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("date",dateString, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("person",person, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("keywords",keywords, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("sentiment",sentiment, Field.Store.YES, Field.Index.ANALYZED));
					document.add(new Field("gender",gender, Field.Store.YES, Field.Index.ANALYZED));
					try {
						iw.addDocument(document);
						iw.commit();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				if (strategy!=IndexingStrategy.INTERNAL){
					Map<String, Object> json = new HashMap<String, Object>();
					json.put("game",game);
					json.put("datadisc",datadisc);
					json.put("part",part);
					json.put("variant",variant);
					json.put("author",author);
					json.put("text",text);
					json.put("date", date);
					json.put("person", person);
					json.put("keywords", keywords);
					json.put("sentiment", sentiment);
					json.put("gender", gender);
					bulkProcessor.add(new IndexRequest("twitter","tweet").source(json));
				}
			}
			infos.remove(id);
		}
		
	}

	@Override
	public void cleanup() {
		try {
			if (iw!=null)
				iw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("scored",new Fields("scored_output","query_id"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}


