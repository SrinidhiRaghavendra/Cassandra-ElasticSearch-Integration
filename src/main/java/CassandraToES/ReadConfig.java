/**
 * Created by sraghavendra on 11/03/16.
 */
package CassandraToES;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.yaml.snakeyaml.*;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;

/**
 * A data object to store and refer the properties required while processing the data.
 */
public class ReadConfig {
    static long FLUSH_INTERVAL;
    static String CASSANDRA_DATA_DIR;
    static String CASSANDRA_CONFIG_FILE;
    static String CASSANDRA_ADDRESS;
    static String CASSANDRA_PORT;
    static String CASSANDRA_USERNAME;
    static String CASSANDRA_PASSWORD;
    static String SYSTEM_TABLES_REQUIRED;
    static String ES_ADDRESS;
    static String ES_PORT;
    static String ES_CLUSTERNAME;
    static ArrayList keyspaces_selected;
    final static String AT = "@";
    final static String COLON = ":";
    final static String DOT = "\\.";


    final String CONFIG_FILE = "configurations.yaml";

    public ReadConfig(String config_file){
        if(config_file.isEmpty()){
            config_file = CONFIG_FILE;
        }
        extractFromConfigFile(config_file);
    }
    public void extractFromConfigFile(String config_file){
        Yaml yaml_file = new Yaml();
        try {
            Map conf_reader;
            conf_reader = (Map) yaml_file.load(new FileReader(new File(config_file)));
            setFlushInterval(conf_reader.get("flush_interval").toString());
            setCassandraDataDir(conf_reader.get("cassandra.data_dir").toString());
            setCassandraConfigFile(conf_reader.get("cassandra.config.yaml").toString());
            setCassandraAddress(conf_reader.get("cassandra.address").toString());
            setCassandraPort(conf_reader.get("cassandra.port") == null ? "" : conf_reader.get("cassandra.port").toString());
            setCassandraUsername(conf_reader.get("cassandra.username") == null ? "" : conf_reader.get("cassandra.username").toString());
            setCassandraPassword(conf_reader.get("cassandra.password") == null ? "" : conf_reader.get("cassandra.password").toString());
            setSystemTableRequirement(conf_reader.get("system_tables.required").toString());

            String[] es = conf_reader.get("elasticsearch.url").toString().split("//");
            String[] name_port = es[es.length - 1].split(COLON);
            setESAddress(name_port[0]);
            setESPort(name_port[1]);

            setESClustername(conf_reader.get("elasticsearch.cluster.name").toString());
            if (conf_reader.get("keyspaces") != null) {
                keyspaces_selected = (ArrayList) conf_reader.get("keyspaces");
            }

            System.setProperty("cassandra.storagedir", getCassandraDataDir());
            System.setProperty("cassandra.config", getCassandraConfigFile());
            DatabaseDescriptor.loadSchemas(false);

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void setFlushInterval(String flush_interval){
        FLUSH_INTERVAL = Long.parseLong(flush_interval);
    }
    public static long getFlushInterval(){
        return FLUSH_INTERVAL;
    }
    public void setCassandraDataDir(String data_dir){
        CASSANDRA_DATA_DIR = data_dir;
    }
    public static String getCassandraDataDir(){
        return CASSANDRA_DATA_DIR;
    }
    public void setCassandraConfigFile(String config_file){
        CASSANDRA_CONFIG_FILE = config_file;
    }
    public static String getCassandraConfigFile(){
        return CASSANDRA_CONFIG_FILE;
    }
    public void setCassandraAddress(String address){
        CASSANDRA_ADDRESS = address;
    }
    public static String getCassandraAddress(){
        return CASSANDRA_ADDRESS;
    }
    public void setCassandraPort(String port){
        CASSANDRA_PORT = port;
    }
    public static String getCassandraPort(){
        return CASSANDRA_PORT;
    }
    public void setCassandraUsername(String username){
        CASSANDRA_USERNAME = username;
    }
    public static String getCassandraUsername(){
        return CASSANDRA_USERNAME;
    }
    public void setCassandraPassword(String password){
        CASSANDRA_PASSWORD = password;
    }
    public static String getCassandraPassword(){
        return CASSANDRA_PASSWORD;
    }
    public void setSystemTableRequirement(String required) {
        SYSTEM_TABLES_REQUIRED = required;
    }
    public static String getSystemTablesRequirement(){
        return SYSTEM_TABLES_REQUIRED;
    }
    public void setESAddress(String address){
        ES_ADDRESS = address;
    }
    public static String getESAddress(){
        return ES_ADDRESS;
    }
    public void setESPort(String port){
        ES_PORT = port;
    }
    public static String getESPort(){
        return ES_PORT;
    }
    public void setESClustername(String clustername){
        ES_CLUSTERNAME = clustername;
    }
    public static String getESClustername(){
        return ES_CLUSTERNAME;
    }
}
