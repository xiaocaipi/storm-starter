package transaction1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.transactional.TransactionalTopologyBuilder;

public class MyTopo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//topo名字  第二个 spout 定义  第三个spout类，第四个  并发度
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbId","spoutid",new MyTxSpout(),1);
		//并行处理的bolt  并发度可以高，这里开3个线程  3个线程处理同一个事务
		builder.setBolt("bolt1", new MyTransactionBolt(),1).shuffleGrouping("spoutid");
		builder.setBolt("committer", new MyCommitter(),1).shuffleGrouping("bolt1") ;
		
		Config conf = new Config() ;
		conf.setDebug(true);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.buildTopology());
		}
		
		
		
		
	}

}
