package transaction1;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
//MyCommitter 做一个汇总 继承了ICommitter 告诉storm 这是一个committer 进行提交事务的时候才会执行finishbatch，什么时候提交这个事务，coordinatorbolt会告诉我们这个事务已经完成了
public class MyCommitter extends BaseTransactionalBolt implements ICommitter{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//最终只有一个汇总值所以可以是个常亮
	public static final String GLOBAL_KEY = "GLOBAL_KEY";
	public static Map<String, DbValue> dbMap = new HashMap<String, DbValue>() ;
	int sum = 0;
	TransactionAttempt id ;
	BatchOutputCollector collector;
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub

		sum += tuple.getInteger(1);
	}

	@Override
	//是committer 所以提交必须是强数据行的 从小到大的，按照事务的
	public void finishBatch() {
		// TODO Auto-generated method stub

		DbValue value = dbMap.get(GLOBAL_KEY);
		DbValue newValue ;
		//如果数据库为空，或者数据库的事务id和当前的事务id不一样
		if (value == null || !value.txid.equals(id.getTransactionId())) {
			//更新数据库
			newValue = new DbValue();
			newValue.txid = id.getTransactionId() ;
			if (value == null) {
				newValue.count = sum ;
			}else {
				newValue.count = value.count + sum ;
			}
			dbMap.put(GLOBAL_KEY, newValue);
		}else
		{
			newValue = value;
		}
		System.out.println("total==========================:"+dbMap.get(GLOBAL_KEY).count);
//		collector.emit(tuple)
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this.id = id ;
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	//DbValue 是存在数据库的类型
	public static class DbValue
	{
		BigInteger txid;
		int count = 0;
	}

}
