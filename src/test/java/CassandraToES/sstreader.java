/**
 * Created by sraghavendra on 25/02/16.
 */
package CassandraToES;

import org.apache.cassandra.config.DatabaseDescriptor;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class sstreader {
    private static final Logger logger = LoggerFactory.getLogger(sstreader.class);
    final String CONFIG_FILE = "configurations.yaml";
    public ArrayList selected = new ArrayList();
    public sstreader(String conf_location)
    {
        String conf_file;
        if(conf_location.isEmpty()) {
            conf_file = CONFIG_FILE;
        }else{
            conf_file = conf_location;
        }
        Yaml yaml = new Yaml();
        try {
            Map conf_reader;
            conf_reader = (Map) yaml.load(new FileReader(new File(conf_file)));
            String[] es = conf_reader.get("elasticsearch.url").toString().split("//");
            String[] name_port = es[es.length-1].split(":");

            System.setProperty("flush_interval",conf_reader.get("flush_interval").toString());
            System.setProperty("data_dir",conf_reader.get("cassandra.data_dir").toString());
            System.setProperty("ES-address",name_port[0]);
            System.setProperty("ES-port",name_port[1]);
            System.setProperty("cluster.name",conf_reader.get("elasticsearch.cluster.name").toString());
            System.setProperty("cassandra.storagedir",conf_reader.get("cassandra.data_dir").toString());
            System.setProperty("cassandra.config",conf_reader.get("cassandra.config.yaml").toString());
            System.setProperty("system",conf_reader.get("system_tables.required").toString());
            System.setProperty("cassandra.address",conf_reader.get("cassandra.address").toString());
            System.setProperty("cassandra.port",conf_reader.get("cassandra.port")==null?"":conf_reader.get("cassandra.port").toString());
            System.setProperty("cassandra.username",conf_reader.get("cassandra.username")==null?"":conf_reader.get("cassandra.username").toString());
            System.setProperty("cassandra.password",conf_reader.get("cassandra.password")==null?"":conf_reader.get("cassandra.password").toString());
            if(conf_reader.get("keyspaces") != null){
                selected = (ArrayList) conf_reader.get("keyspaces");
                logger.debug("",selected);
            }
        }catch (Exception e){e.printStackTrace();}
        DatabaseDescriptor.loadSchemas(false);
    }
    public static void main(String args[]) {
        String conf;
        if(args.length==0) {
            conf = "";
        }else{
            conf = args[0];
        }
        new sstreader("").timedExecution(Integer.parseInt(System.getProperty("flush_interval")));
    }
    private void timedExecution(int seconds)
    {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new getDelta(selected),1000,seconds*1000);

    }
}
