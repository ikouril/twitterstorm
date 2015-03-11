package cz.vutbr.fit.twitterstorm.topologies;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.vutbr.fit.twitterstorm.bolts.GenderBolt;
import cz.vutbr.fit.twitterstorm.bolts.LemmaBolt;
import cz.vutbr.fit.twitterstorm.bolts.NERBolt;
import cz.vutbr.fit.twitterstorm.bolts.POSBolt;
import cz.vutbr.fit.twitterstorm.bolts.ParseBolt;
import cz.vutbr.fit.twitterstorm.bolts.SentenceSplitterBolt;
import cz.vutbr.fit.twitterstorm.bolts.SentimentBolt;
import cz.vutbr.fit.twitterstorm.bolts.TokenizerBolt;
import cz.vutbr.fit.twitterstorm.bolts.FilterBolt;
import cz.vutbr.fit.twitterstorm.bolts.IndexBolt;
import cz.vutbr.fit.twitterstorm.bolts.MergerBolt;
import cz.vutbr.fit.twitterstorm.spouts.DumpSpout;
import cz.vutbr.fit.twitterstorm.spouts.QuerySpout;
import cz.vutbr.fit.twitterstorm.spouts.TwitterSpout;
import cz.vutbr.fit.twitterstorm.util.IndexingStrategy;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
/**
 * Local TwitterStorm topology
 * @author ikouril
 */
public class TwitterStormTopology {
	
	public static final int PARALLELISM=4;
	public static final int GAMES=195;
	public static final int BLOCK=100;
	public static String[] keywords=new String[GAMES];
	public static String[][] variants=new String[GAMES][];
	public static String[][] parts=new String[GAMES][];
	public static String[][] datadiscs=new String[GAMES][];
	public static Map<String,Integer> gameMap=new HashMap<String,Integer>();
	public static String pattern;
	public static StanfordCoreNLP pipeline;
	public static TransportClient client;
	
	static{
		Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "athena1").build();
        client =    new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress("athena1.fit.vutbr.cz", 9300));
        
        StringBuilder patternBuilder=new StringBuilder();
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena1.fit.vutbr.cz/twitterstorm/allgames.txt").openStream()));
			String line=reader.readLine();
			int cnt=0;
			patternBuilder.append("(");
			while (line!=null){
				keywords[cnt]=line;
				gameMap.put(line, cnt);
				cnt++;
				patternBuilder.append(line+"|");
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        int variantCounter=0;
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena1.fit.vutbr.cz/twitterstorm/name_variations.txt").openStream()));
			String line=reader.readLine();
			while (line!=null){
				List<String> vals=new ArrayList<String>();
				String[] vars=line.split("\t");
				for (String var:vars){
					vals.add(var);
				}
				Collections.sort(vals, new Comparator<String>() {

					@Override
					public int compare(String o1, String o2) {
						return o2.length() - o1.length();
					}
				});
				String[] resultArray=new String[vals.size()];
				variants[variantCounter++]=vals.toArray(resultArray);
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        int partCounter=0;
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena1.fit.vutbr.cz/twitterstorm/name_part_variations.txt").openStream()));
			String line=reader.readLine();
			while (line!=null){
				if (line.length()>0){
					List<String> vals=new ArrayList<String>();
					String[] vars=line.split("\t");
					for (String var:vars){
						vals.add(var);
					}
					Collections.sort(vals, new Comparator<String>() {

						@Override
						public int compare(String o1, String o2) {
							return o2.length() - o1.length();
						}
					});
					String[] resultArray=new String[vals.size()];
					parts[partCounter++]=vals.toArray(resultArray);
				}
				else{
					parts[partCounter++]=null;
				}
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        
        int datadiscCounter=0;
        try{
	        BufferedReader reader=new BufferedReader(new InputStreamReader(new URL("http://athena1.fit.vutbr.cz/twitterstorm/datadisc_variants.txt").openStream()));
			String line=reader.readLine();
			while (line!=null){
				if (line.length()>0){
					List<String> vals=new ArrayList<String>();
					String[] vars=line.split("\t");
					for (String var:vars){
						vals.add(var);
					}
					Collections.sort(vals, new Comparator<String>() {

						@Override
						public int compare(String o1, String o2) {
							return o2.length() - o1.length();
						}
					});
					String[] resultArray=new String[vals.size()];
					datadiscs[datadiscCounter++]=vals.toArray(resultArray);
				}
				else{
					datadiscs[datadiscCounter++]=null;
				}
				line=reader.readLine();
			}
			reader.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        }
        patternBuilder.setCharAt(patternBuilder.length()-1, ')');
        pattern=patternBuilder.toString();
        
        Properties props=new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment, gender, ner");
		pipeline=new StanfordCoreNLP(props);
	}
	
	public static void main(String[] params){
		Logger logger = LoggerFactory.getLogger(TwitterStormTopology.class);
        logger.debug("TOPOLOGY START");
        
        String deploymentId=UUID.randomUUID().toString();

        QuerySpout query=new QuerySpout();
        //TwitterSpout twitter=new TwitterSpout(BLOCK);
        DumpSpout dump=new DumpSpout(BLOCK,deploymentId);
        TokenizerBolt tokenizer=new TokenizerBolt(deploymentId);
        SentenceSplitterBolt sentenceSplitter=new SentenceSplitterBolt(deploymentId);
        POSBolt posBolt=new POSBolt(deploymentId);
        
        //IndexingStrategy decides where to store results (INTERNAL - lucene, EXTERNAL - elasticsearch or BOTH)
        IndexBolt index=new IndexBolt(deploymentId,IndexingStrategy.BOTH,false);
        GenderBolt gender=new GenderBolt(deploymentId);
        LemmaBolt lemma=new LemmaBolt(deploymentId);
        NERBolt ner=new NERBolt(deploymentId);
        ParseBolt parser=new ParseBolt(deploymentId);
        SentimentBolt sentiment=new SentimentBolt(deploymentId);
        FilterBolt filter=new FilterBolt(deploymentId);
        MergerBolt merger=new MergerBolt();
        TopologyBuilder builder = new TopologyBuilder();
        
        //builder.setSpout("twitter_spout", twitter, 1);
        builder.setSpout("dump_spout", dump, PARALLELISM);
        builder.setSpout("query_spout", query, 1);
        
        //can make it higher than PARALELISM, if slow ...
        //builder.setBolt("filter_bolt", filter, PARALLELISM).shuffleGrouping("twitter_spout", "twitter");
        builder.setBolt("filter_bolt", filter, PARALLELISM).shuffleGrouping("dump_spout", "dump");
        
        //PARALLELISM is a constant mainly for merger to know how many inputs with one id is expected
        builder.setBolt("tokenizer_bolt", tokenizer, PARALLELISM).shuffleGrouping("filter_bolt", "process");
        builder.setBolt("sentenceSplitter_bolt", sentenceSplitter,PARALLELISM).shuffleGrouping("tokenizer_bolt","tokenized");
        builder.setBolt("pos_bolt", posBolt,PARALLELISM).shuffleGrouping("sentenceSplitter_bolt","sentenced");
        builder.setBolt("gender_bolt", gender,PARALLELISM).shuffleGrouping("pos_bolt","to_gender");
        builder.setBolt("lemma_bolt", lemma,PARALLELISM).shuffleGrouping("pos_bolt","to_keywords");
        builder.setBolt("ner_bolt", ner,PARALLELISM).shuffleGrouping("pos_bolt","to_ner");
        builder.setBolt("parse_bolt", parser,PARALLELISM).shuffleGrouping("pos_bolt","to_sentiment");
        builder.setBolt("sentiment_bolt", sentiment,PARALLELISM).shuffleGrouping("parse_bolt", "parsed");
        builder.setBolt("index_bolt", index, PARALLELISM).allGrouping("query_spout","query")
        .fieldsGrouping("filter_bolt","filtered",new Fields("id"))
        .fieldsGrouping("gender_bolt","gendered",new Fields("id"))
        .fieldsGrouping("lemma_bolt", "keywords",new Fields("id"))
        .fieldsGrouping("ner_bolt","persons",new Fields("id"))
        .fieldsGrouping("sentiment_bolt","sentimented",new Fields("id"));
        builder.setBolt("merger_bolt", merger).shuffleGrouping("index_bolt","scored");
        
        Config conf = new Config();
        conf.setDebug(true);

        
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitterstorm", conf, builder.createTopology());
        
	}

}
