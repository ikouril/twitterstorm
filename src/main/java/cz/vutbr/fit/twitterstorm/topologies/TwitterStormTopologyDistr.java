package cz.vutbr.fit.twitterstorm.topologies;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.vutbr.fit.twitterstorm.bolts.FilterBolt;
import cz.vutbr.fit.twitterstorm.bolts.GenderBolt;
import cz.vutbr.fit.twitterstorm.bolts.LemmaBolt;
import cz.vutbr.fit.twitterstorm.bolts.NERBolt;
import cz.vutbr.fit.twitterstorm.bolts.POSBolt;
import cz.vutbr.fit.twitterstorm.bolts.ParseBolt;
import cz.vutbr.fit.twitterstorm.bolts.SentenceSplitterBolt;
import cz.vutbr.fit.twitterstorm.bolts.SentimentBolt;
import cz.vutbr.fit.twitterstorm.bolts.TokenizerBolt;
import cz.vutbr.fit.twitterstorm.bolts.IndexBolt;
import cz.vutbr.fit.twitterstorm.bolts.MergerBolt;
import cz.vutbr.fit.twitterstorm.spouts.DumpSpout;
import cz.vutbr.fit.twitterstorm.spouts.QuerySpout;
import cz.vutbr.fit.twitterstorm.spouts.TwitterSpout;
import cz.vutbr.fit.twitterstorm.util.IndexingStrategy;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
/**
 * Distributed PetaFuel topology
 * @author ikouril
 */
public class TwitterStormTopologyDistr {
	
	public static final int PARALLELISM=4;
	public static final int BLOCK=100;
	
	public static void main(String[] params) throws AlreadyAliveException, InvalidTopologyException{
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
        IndexBolt index=new IndexBolt(deploymentId,IndexingStrategy.BOTH,true);
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

        
        //final LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("twitterstorm", conf, builder.createTopology());
        StormSubmitter.submitTopology("twitterstorm",conf, builder.createTopology());
        
	}

}
