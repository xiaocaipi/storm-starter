package visits;

import java.net.InetAddress;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PVBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//创建一个目录 把一个线程的id写进去，每次输出都去拿id对比下
	public static final String zk_path = "/lock/storm/pv";
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			zKeeper.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	String logString = null;
	String lockData = null;
	String session_id = null;
	ZooKeeper zKeeper = null;
	
	long Pv = 0;
	long beginTime = System.currentTimeMillis() ;
	long endTime = 0;
	
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stubr
		try {
			endTime = System.currentTimeMillis() ;
			logString = input.getString(0);
			if (logString != null) {
				session_id = logString.split("\t")[1];
				if (session_id != null) {
					Pv ++ ;
				}
			}
			if (endTime - beginTime >= 5 * 1000) {
				System.err.println(lockData+" ======================== ");
				if (lockData.equals(new String(zKeeper.getData(zk_path, false, null)))) {
					
					System.err.println("pv ======================== "+ Pv * 4);
				}
				beginTime = System.currentTimeMillis() ;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		try {//第二个参数是3秒连不到算超时
			zKeeper = new ZooKeeper("192.168.1.105:2181,192.168.1.106:2181",3000,new Watcher(){

				@Override
				public void process(WatchedEvent event) {
					System.out.println("event:"+event.getType());
					
				}
			});
			//用循环去判断，zookeeper确实连上了，不连上就sleep下
			while (zKeeper.getState() != ZooKeeper.States.CONNECTED) {
				Thread.sleep(1000);
			}
			
			InetAddress address = InetAddress.getLocalHost();
			//ip地址和task的肯定是唯一的
			lockData = address.getHostAddress() + ":" +context.getThisTaskId() ;
			
			if(zKeeper.exists(zk_path, false) == null)
			{	//创建文件
				zKeeper.create(zk_path, lockData.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
			
		} catch (Exception e) {
			try {
				zKeeper.close();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
