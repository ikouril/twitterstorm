package cz.vutbr.fit.twitterstorm.topologies;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

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
	
	public static void main(String[] params) throws AlreadyAliveException, InvalidTopologyException, JSAPException{
		SimpleJSAP jsap = new SimpleJSAP( TwitterStormTopology.class.getName(), "Processes stream of tweets.",
				new Parameter[] {
					new FlaggedOption( "blockSize", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 'b', "blockSize", "Number of tweets to be processed in one block." ),
					new FlaggedOption( "parallelism", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 'p', "paralelism", "Number parallel bolts." ),
					new FlaggedOption( "indexing",JSAP.STRING_PARSER,"internal",JSAP.NOT_REQUIRED,'i',"indexing","Indexing strategy."),
					new FlaggedOption( "files", JSAP.STRING_PARSER, "athena1.fit.vutbr.cz/twitterstorm/dump.json;athena2.fit.vutbr.cz/twitterstorm/dump.json;athena3.fit.vutbr.cz/twitterstorm/dump.json;athena4.fit.vutbr.cz/twitterstorm/dump.json;athena5.fit.vutbr.cz/twitterstorm/dump.json;athena6.fit.vutbr.cz/twitterstorm/dump.json;knot01.fit.vutbr.cz/twitterstorm/dump.json;knot02.fit.vutbr.cz/twitterstorm/dump.json;knot03.fit.vutbr.cz/twitterstorm/dump.json;knot04.fit.vutbr.cz/twitterstorm/dump.json", JSAP.NOT_REQUIRED, 'f', "Adresses of twitter dumps." )
				}
		);
		

		JSAPResult jsapResult = jsap.parse( params );
		
		TwitterStormTopology.BLOCK=jsapResult.getInt("blockSize");
		TwitterStormTopology.PARALLELISM=jsapResult.getInt("parallelism");
		//parallelism should be equal to length of files array
		TwitterStormTopology.FILES=jsapResult.getString("files").split(";");
		
		String strategyString=jsapResult.getString("indexing");
		IndexingStrategy strategy=strategyString.equals("internal")?IndexingStrategy.INTERNAL:strategyString.equals("external")?IndexingStrategy.EXTERNAL:IndexingStrategy.BOTH;

		Logger logger = LoggerFactory.getLogger(TwitterStormTopology.class);
        logger.debug("TOPOLOGY START");
        
        String deploymentId=UUID.randomUUID().toString();

        QuerySpout query=new QuerySpout();
        //TwitterSpout twitter=new TwitterSpout(BLOCK);
        DumpSpout dump=new DumpSpout(TwitterStormTopology.BLOCK,deploymentId);
        TokenizerBolt tokenizer=new TokenizerBolt(deploymentId);
        SentenceSplitterBolt sentenceSplitter=new SentenceSplitterBolt(deploymentId);
        POSBolt posBolt=new POSBolt(deploymentId);
        
        //IndexingStrategy decides where to store results (INTERNAL - lucene, EXTERNAL - elasticsearch or BOTH)
        //In case of internal strategy, last parameter indicates, whether RAMDirectory should be used
        IndexBolt index=new IndexBolt(deploymentId,strategy,true);
        GenderBolt gender=new GenderBolt(deploymentId);
        LemmaBolt lemma=new LemmaBolt(deploymentId);
        NERBolt ner=new NERBolt(deploymentId);
        ParseBolt parser=new ParseBolt(deploymentId);
        SentimentBolt sentiment=new SentimentBolt(deploymentId);
        FilterBolt filter=new FilterBolt(deploymentId);
        MergerBolt merger=new MergerBolt();
        TopologyBuilder builder = new TopologyBuilder();
        
        //builder.setSpout("twitter_spout", twitter, 1);
        builder.setSpout("dump_spout", dump, TwitterStormTopology.PARALLELISM);
        builder.setSpout("query_spout", query, 1);
        
        //can make it higher than PARALELISM, if slow ...
        //builder.setBolt("filter_bolt", filter, TwitterStormTopology.PARALLELISM).shuffleGrouping("twitter_spout", "twitter");
        builder.setBolt("filter_bolt", filter, TwitterStormTopology.PARALLELISM).shuffleGrouping("dump_spout", "dump");
        
        //TwitterStormTopology.PARALLELISM is a constant mainly for merger to know how many inputs with one id is expected
        builder.setBolt("tokenizer_bolt", tokenizer, TwitterStormTopology.PARALLELISM).shuffleGrouping("filter_bolt", "process");
        builder.setBolt("sentenceSplitter_bolt", sentenceSplitter,TwitterStormTopology.PARALLELISM).shuffleGrouping("tokenizer_bolt","tokenized");
        builder.setBolt("pos_bolt", posBolt,TwitterStormTopology.PARALLELISM).shuffleGrouping("sentenceSplitter_bolt","sentenced");
        builder.setBolt("gender_bolt", gender,TwitterStormTopology.PARALLELISM).shuffleGrouping("pos_bolt","to_gender");
        builder.setBolt("lemma_bolt", lemma,TwitterStormTopology.PARALLELISM).shuffleGrouping("pos_bolt","to_keywords");
        builder.setBolt("ner_bolt", ner,TwitterStormTopology.PARALLELISM).shuffleGrouping("pos_bolt","to_ner");
        builder.setBolt("parse_bolt", parser,TwitterStormTopology.PARALLELISM).shuffleGrouping("pos_bolt","to_sentiment");
        builder.setBolt("sentiment_bolt", sentiment,TwitterStormTopology.PARALLELISM).shuffleGrouping("parse_bolt", "parsed");
        builder.setBolt("index_bolt", index, TwitterStormTopology.PARALLELISM).allGrouping("query_spout","query")
        .fieldsGrouping("filter_bolt","filtered",new Fields("id"))
        .fieldsGrouping("gender_bolt","gendered",new Fields("id"))
        .fieldsGrouping("lemma_bolt", "keywords",new Fields("id"))
        .fieldsGrouping("ner_bolt","persons",new Fields("id"))
        .fieldsGrouping("sentiment_bolt","sentimented",new Fields("id"));
        builder.setBolt("merger_bolt", merger).shuffleGrouping("index_bolt","scored");
        
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(8);
        
        //final LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("twitterstorm", conf, builder.createTopology());
        StormSubmitter.submitTopology("twitterstorm",conf, builder.createTopology());
        
	}

}
