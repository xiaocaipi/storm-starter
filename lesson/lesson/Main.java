package lesson;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
			
		TopologyBuilder builder = new TopologyBuilder();
		//第三个参数是并行度 大数据的时候这里需要设置下，也就是线程数
		//如果spout 这里改成2的话，数据会读2份，spout开2个线程都会把文件读一遍，如果数据源是消息队列的话，消费完之后，数据就消费不到了，就不会有2分
		//所以spout读文件只能是1
		builder.setSpout("spout", new MySpout(), 1);
		//.shuffleGrouping("spout") 定义数据源来自 spout 对应上面设置的
		//这里改2用2个线程，bolt起2个线程来处理，因为是shuffleGrouping 是轮训的 所以每个线程会处理25条记录，一共的记录有50条
		//bolt 是可以开多线程的，因为shfflegrouping 是轮训的
//		builder.setBolt("bolt", new MyBolt(), 1).shuffleGrouping("spout");
		
		//noneGrouping  演示
//		builder.setBolt("bolt", new MyBolt(), 2).noneGrouping("spout");
		
		//noneGrouping  演示  需要执行fields，这里的field 是上一个spout 发送出来定义的格式
		builder.setBolt("bolt", new MyBolt(), 2).fieldsGrouping("spout",new Fields("log"));

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
