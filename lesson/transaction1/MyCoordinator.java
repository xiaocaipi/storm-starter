package transaction1;

import java.math.BigInteger;

import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

public class MyCoordinator implements ITransactionalSpout.Coordinator<MyMata>{

	public static int BATCH_NUM = 3 ;
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	//启动一个事务，生成一个元数据
	public MyMata initializeTransaction(BigInteger txid, MyMata prevMetadata) {
		// TODO Auto-generated method stub
		//prevMetadata  可以从上一个元数据获取这个元数据的开始位置
		long beginPoint = 0;
		//如果 上一个是空，说明程序刚开始就是0
		if (prevMetadata == null) {
			beginPoint = 0 ;
		}else {
			//不为空的话，这个元数据的开始位置就是 上一个的开始加上一个处理了多少
			beginPoint = prevMetadata.getBeginPoint() + prevMetadata.getNum() ;
		}
		
		MyMata mata = new MyMata() ;
		mata.setBeginPoint(beginPoint);
		mata.setNum(BATCH_NUM);
		System.err.println("启动一个事务："+mata.toString());
		return mata;
	}

	@Override
	//isReady 返回true就可以 true的时候 事务才会开始
	public boolean isReady() {
		Utils.sleep(2000);
		return true;
	}

}
