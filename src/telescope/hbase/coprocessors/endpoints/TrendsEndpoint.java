package telescope.hbase.coprocessors.endpoints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import telescope.hbase.coprocessors.endpoints.protocols.TrendsProtocol;

public class TrendsEndpoint extends BaseEndpointCoprocessor implements TrendsProtocol {

	@Override
	public HashMap<String, Long> getData(String topic) throws IOException {
		
		HashMap<String, Long> keywordMap = new HashMap<String, Long>();		
		
		long now = System.currentTimeMillis();
		
		String startKey = topic + "-" + (9000000000000L - now);		
		//System.out.println("Start Row Key: " + startKey);
		String endKey = topic + "-" + (9000000000000L - (now - (86400000 * 6))) + "-a";
		//System.out.println("End Row Key: " + endKey);	
		
		byte[] byteStartKey = Bytes.toBytes(startKey);
		byte[] byteEndKey = Bytes.toBytes(endKey);
		
		byte[] family = Bytes.toBytes("m");
		byte[] qualifier = Bytes.toBytes("message");
		
		Scan s = new Scan();
		s.setStartRow(byteStartKey);
		s.setStopRow(byteEndKey); 
		s.addColumn(family, qualifier);
		s.setMaxVersions(1);
		//Igor's code suggestion
		s.setCacheBlocks(false);
		s.setCaching(500);
		s.setBatch(100);		
		//End Igor's code suggestion		
		
		RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();
		InternalScanner scanner = environment.getRegion().getScanner(s);
		try {
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for(KeyValue pair : curVals){
					byte[] buffer = pair.getBuffer();
										
					String valueString = Bytes.toString(buffer, pair.getValueOffset(), pair.getValueLength());
					//System.out.println("value: " + valueString);					
					
					String[] splitMsg = valueString.split("[\\p{Punct}]*\\s");
					
					for(String fragment : splitMsg){
						if(fragment.length() > 0){
							//System.out.println("Testing: " + fragment);
							if(fragment.charAt(0) == '#'){
								//System.out.println("Success on" + fragment);
								if(keywordMap.containsKey(fragment)){
									keywordMap.put(fragment, (keywordMap.get(fragment) + 1));
								}else{
									keywordMap.put(fragment, 1L);
								}
							}
						}else{
							//System.out.println("Empty fragment");
						}
					}
				}
				
			}while(done);
		}finally {
			scanner.close();
		}			
		
		return keywordMap;
	}

}
