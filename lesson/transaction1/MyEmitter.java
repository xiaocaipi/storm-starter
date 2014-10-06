package transaction1;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

public class MyEmitter implements ITransactionalSpout.Emitter<MyMata>{
	//dbMap 是数据源
	Map<Long, String> dbMap  = null;
	public MyEmitter(Map<Long, String> dbMap) {
		this.dbMap = dbMap;
	}
	@Override
	public void cleanupBefore(BigInteger txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	//TransactionAttempt 这个发送数据的第一个必须是它
	//emitBatch  发送一个批次的数据 批次开始结束从metadata里面定义，数据从dbmap里面来定义，dbmap里面是所有的数据，就相当于从数据库里面读了数据存放在map里面，数据源的获取和普通的spout一样
	public void emitBatch(TransactionAttempt tx, MyMata coordinatorMeta,
			BatchOutputCollector collector) {
		// TODO Auto-generated method stub
		long beginPoint = coordinatorMeta.getBeginPoint() ;
		int num = coordinatorMeta.getNum() ;
		
		for (long i = beginPoint; i < num+beginPoint; i++) {
			if (dbMap.get(i)==null) {
				continue;
			}
			collector.emit(new Values(tx,dbMap.get(i)));
		}
	}
	
	

}
