package lesson;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout implements IRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;			

	SpoutOutputCollector collector = null;
	
	
	String str = null;

	@Override
	public void nextTuple() {
		try {
			while ((str = this.br.readLine()) != null) {
				// 过滤动作
				
				collector.emit(new Values(str));
				
//				Thread.sleep(3000);
				//to do 
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		try {
			this.collector = collector;
			this.fis = new FileInputStream("track.log");
			this.isr = new InputStreamReader(fis, "UTF-8");
			this.br = new BufferedReader(isr);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		
	}

	@Override
	//定义发射数据的字段，格式  现在的一行www.taobao.com	VVVYH6Y4V4SFXZ56JIPDPB4V678	2014-01-07 08:40:52
	//是这样的，这里把它一行都定义成一个字段log ，可以定义成多列
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("spout ack:"+msgId.toString());
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		try {
			br.close();
			isr.close();
			fis.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("spout fail:"+msgId.toString());
		
	}

}
