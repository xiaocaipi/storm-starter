package lesson;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();
		//第三个参数是并行度 大数据的时候这里需要设置下，也就是线程数
		//如果
		builder.setSpout("spout", new MySpout(), 1);
		//.shuffleGrouping("spout") 定义数据源来自 spout 对应上面设置的
		//这里改2用2个线程，bolt起2个线程来处理，因为是shuffleGrouping 是轮训的 所以每个线程会处理25条记录，一共的记录有50条
		builder.setBolt("bolt", new MyBolt(), 2).shuffleGrouping("spout");

		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 4);

		if (args.length > 0) {
			try {
				//分布式提交，分布式提交一般名称都会当参数穿进去
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			//本地模式，本地模式会报连接不到zookeeper 报错，这是不要紧的
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}
		
		
		
		

	}

}
