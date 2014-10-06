package transaction1;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyTransactionBolt extends BaseTransactionalBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Integer count = 0;
	BatchOutputCollector collector;
	TransactionAttempt tx ;
	@Override
	//execute 会从emmitter里面获得每一行进行处理，同一个批次处理完毕了会交给finishbatch
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub

		tx = (TransactionAttempt)tuple.getValue(0);
		System.err.println("MyTransactionBolt TransactionAttempt "+tx.getTransactionId() +"  attemptid"+tx.getAttemptId());
		String log = tuple.getString(1);
		if (log != null && log.length()>0) {
			count ++ ;
		}
		
	}

	@Override
	//因为是事务型的统计，所以放在finishbatch 里面做发送数据，如果放在execute里面发送数据和普通的也就没有区别了
	public void finishBatch() {
		// TODO Auto-generated method stub
		System.err.println("finishBatch "+count );
		collector.emit(new Values(tx,count));
	}

	@Override
	//prepare 传进来的TransactionAttempt 就是当前的事务，和emmitter 发过来的事务tx是一样的
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this.collector = collector;
		System.err.println("MyTransactionBolt prepare "+id.getTransactionId() +"  attemptid"+id.getAttemptId());
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("tx","count"));
	}

}
