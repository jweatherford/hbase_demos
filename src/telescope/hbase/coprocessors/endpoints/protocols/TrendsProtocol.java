package telescope.hbase.coprocessors.endpoints.protocols;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface TrendsProtocol extends CoprocessorProtocol{
	HashMap<String, Long> getData(String topic) throws IOException;
}
