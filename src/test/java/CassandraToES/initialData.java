/**
 * Created by sraghavendra on 09/03/16.
 */
package CassandraToES;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.*;
import java.io.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.cassandra.config.DatabaseDescriptor;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Timer;

import org.yaml.snakeyaml.Yaml;
public class initialData {
    private static final Logger logger = LoggerFactory.getLogger(initialData.class);
    static ArrayList selected = new ArrayList();

    public static void main(String []args){
        sstreader reader;
        if(args.length==0) {
            reader = new sstreader("");
        }else{
            reader = new sstreader(args[0]);
        }
        selected = reader.selected;
        processInitialData();
    }
    public static void processInitialData() {
        getDelta methods = new getDelta();
        if(selected.size()==0){
            methods.processData(System.getProperty("data_dir") + "data");
        }else
        {
            Map map = new HashMap();
            ArrayList tables = new ArrayList();
            for(int i=0;i<selected.size();i++){
                logger.debug("selected keyspaces");
                map = (Map)selected.get(i);
                tables = map.get("tables")==null?null:(ArrayList) map.get("tables");
                String[] table_names;
                if(tables==null) {
                    table_names = null;
                }else {
                    table_names = new String[tables.size()];
                    for (int j = 0; j < tables.size(); j++) {
                        table_names[j] = ((Map) tables.get(j)).get("name").toString();
                    }
                }
                methods.processKeyspace(new File(System.getProperty("data_dir")+"data/"+map.get("name")),tables,table_names,true);
            }
        }
    }
}
