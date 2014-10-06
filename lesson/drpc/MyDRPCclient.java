package drpc;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class MyDRPCclient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		DRPCClient client = new DRPCClient("192.168.1.105", 3772);
		try {
			String result = client.execute("exclamation", "hello ");
			
			System.out.println(result);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DRPCExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}

}
