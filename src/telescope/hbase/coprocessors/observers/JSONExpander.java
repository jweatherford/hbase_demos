package telescope.hbase.coprocessors.observers;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JSONExpander extends BaseRegionObserver {
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		Configuration c = env.getConfiguration();
		families = c.get("families", "").split(":");
		logger.info("Loaded " + families.length + " families from arguments " + c.get("families"));
	}

	
	/**
	 * This class is used to transform the single column json value into
	 * a multi column (and multi column family) twitter request
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) {
		if(families.length == 0) { 
			logger.warn("No families configured for this coprocessors. Please set 'families=XXX' in the coprocessor definition");
			return;
		}
		
		//we require the first family to be where the raw data is
		byte[] FAMILY = families[0].getBytes();
		byte[] JSON_COLUMN = "json_raw".getBytes();
		
		//If the json_raw column is not present, then we should skip this
		if(!put.has(FAMILY, JSON_COLUMN)) {
			return;
		}
			
		String json = Bytes.toString(put.get(FAMILY, JSON_COLUMN).get(0).getValue());
		ObjectMapper mapper = new ObjectMapper();
		LinkedHashMap<String, ?> jsonmap;
		try {
			jsonmap = (LinkedHashMap<String, ?>)mapper.readValue(json, Map.class);		
			
			for(String s_family : families) {
				byte[] family = s_family.getBytes();
				LinkedHashMap<String, ?> columns = (LinkedHashMap<String, ?>) jsonmap.get(s_family);
				
				if(columns == null) {
					logger.error("ERROR NULL COLUMNS from getting the " + s_family + " out of json " + json);
				}
					
				for(Entry<String, ?> column : columns.entrySet()) {
					if(column.getValue() instanceof Boolean) {
						Boolean value = (Boolean) column.getValue();
						put.add(family, Bytes.toBytes(column.getKey()), Bytes.toBytes(value));
					} else if(column.getValue() instanceof Long) {
						Long value = (Long) column.getValue();
						put.add(family, Bytes.toBytes(column.getKey()), Bytes.toBytes(value));
					} else if(column.getValue() instanceof Integer) {
						Integer value = (Integer) column.getValue();
						put.add(family, Bytes.toBytes(column.getKey()), Bytes.toBytes(value));
					} else if(column.getValue() instanceof String) {
						String value = (String) column.getValue();
						put.add(family, Bytes.toBytes(column.getKey()), Bytes.toBytes(value));
					} else if(column.getValue() instanceof String[]) {
						String[] values = (String[]) column.getValue();
						StringBuffer compact = new StringBuffer();
						for(String value : values) {
							compact.append(value).append(",");
						}
						put.add(family, Bytes.toBytes(column.getKey()), Bytes.toBytes(compact.toString()));
					} else {
						//logger.warn("Json value is not of type Long/Integer/String/Boolean for " + column.getKey() +  ":"  + column.getValue());
					}
				}
			}
			
				
		} catch (JsonParseException e1) {
			logger.warn("Error parsing json " + e1.getMessage(), e1);
		} catch (JsonMappingException e1) {
			logger.warn("Error mapping json " + e1.getMessage(), e1);
		} catch (IOException e1) {
			logger.warn("Generic IOException " + e1.getMessage(), e1);
		}
		
		
		//delete the json data so we don't double write it
		put.add(FAMILY, JSON_COLUMN, "--removed--".getBytes());

	}

	
	String[] families;
	Logger logger = Logger.getLogger(JSONExpander.class);

	
}
