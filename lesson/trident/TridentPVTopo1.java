package trident;

import java.util.Random;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import trident.functions.MyFunction;
import trident.functions.MySplit;
import trident.functions.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class TridentPVTopo1 {
	//传进来的是个drpc的参数，说明用drpc的方式来查结果
  public static StormTopology buildTopology(LocalDRPC drpc) {
	  Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
				"2014-01-07 09:40:49", "2014-01-08 10:40:49", "2014-01-08 11:40:49", "2014-01-07 12:40:49" };
	 
		//spout 就用原来的spout  第一个
     FixedBatchSpout spout = new FixedBatchSpout(new Fields("a","b","c"), 3, 
    	new Values("1","2","3"),
    	new Values("4","1","6"),
    	new Values("3","0","8"));
     TridentTopology topology = new TridentTopology();
     
    spout.setCycle(false);
     Stream stram=topology.newStream("spout1", spout)
	    	.each(new Fields("b"),new MyFunction(), new Fields("d"));
     System.out.println(stram.toString());


//    topology.newDRPCStream("GetPV", drpc).each(new Fields("a","b","c"), new Fields("a","b","c")).each(new Fields("b"), new MyFunction(), new Fields("d"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      System.err.println("DRPC RESULT: " + drpc.execute("GetPV", "2014-01-07 2014-01-08"));
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }
}
