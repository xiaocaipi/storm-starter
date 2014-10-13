package trident;

import java.util.Random;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import trident.functions.MySplit;
import trident.functions.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class TridentPVTopo {
	//传进来的是个drpc的参数，说明用drpc的方式来查结果
  public static StormTopology buildTopology(LocalDRPC drpc) {
	  Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
				"2014-01-07 09:40:49", "2014-01-08 10:40:49", "2014-01-08 11:40:49", "2014-01-07 12:40:49" };
	 
		//spout 就用原来的spout  第一个
     FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachLog"), 3, 
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]));
    
    spout.setCycle(false);

    TridentTopology topology = new TridentTopology();
    //建立一个TridentState spout1 是给一个名称，输入是上面构建的spout，注释掉的并发度
    TridentState wordCounts = topology.newStream("spout1", spout)
//    	.parallelismHint(16)
    		//写一个自己的split
    	.each(new Fields("eachLog"),new MySplit("\t"), new Fields("date","session_id"))
    	//  persistentAggregate 的参数是个StateFactory 
    	//groupBy 之后会变成一个GroupedStream 这里对date分组  persistentAggregate 是GroupedStream 方法  这里用4个参数的，传fileds 这里对session_id 进行分组 输出是个pv
    	.groupBy(new Fields("date")).persistentAggregate(new MemoryMapState.Factory(),new Fields("session_id"),new Count(), new Fields("PV"));
//        .parallelismHint(16);
    //newDRPCStream 第一个参数是函数名称
    topology.newDRPCStream("GetPV", drpc).each(new Fields("args"), new Split(" "), new Fields("date"))
    	//根据日期来进行分组
        .groupBy(new Fields("date"))
        //从wordCounts 查询 第二个是输入字段 MapGet 是从wordCounts 根据date get一下  为pv
        .stateQuery(wordCounts, new Fields("date"), new MapGet(), new Fields("PV"))
        .each(new Fields("PV"),new FilterNull());
//        .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
    	  //调用Getpv函数
    	  //DRPC RESULT: [["2014-01-07 2014-01-08","2014-01-07",7],["2014-01-07 2014-01-08","2014-01-08",1]]  输出 
    	  //"2014-01-07 2014-01-08" 是查询参数  "2014-01-07",7是第一个结果  "2014-01-08",1 第二个参数结果
        System.err.println("DRPC RESULT: " + drpc.execute("GetPV", "2014-01-07 2014-01-08"));
        Thread.sleep(1000);
      }
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }
}
