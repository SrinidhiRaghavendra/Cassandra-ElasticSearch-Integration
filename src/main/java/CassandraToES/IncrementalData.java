/**
 * Created by sraghavendra on 25/02/16.
 */
package CassandraToES;

import org.apache.cassandra.config.DatabaseDescriptor;
import java.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to process the delta of the cassandra in am interval of time.
 */
public class IncrementalData {
    private static final Logger logger = LoggerFactory.getLogger(IncrementalData.class);

    public static void main(String args[]) {
        String conf_file;
        if(args.length==0){
            conf_file="";
        }else{
            conf_file = args[0];
        }
        new ReadConfig(conf_file);//Read the configuration file.
        timedExecution(ReadConfig.getFlushInterval());//Call the method which runs periodically.
    }

    private static void timedExecution(long seconds) {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new ProcessCassandraData(), 1000, seconds * 1000);//The method is scheduled at a fixed rate of "seconds" seconds.

    }
}
