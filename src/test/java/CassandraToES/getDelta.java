/**
 * Created by sraghavendra on 29/02/16.
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


public class getDelta extends TimerTask{
    private static final Logger logger = LoggerFactory.getLogger(getDelta.class);
    private ArrayList selected = new ArrayList();
    static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    static NodeProbe probe;
    public getDelta(ArrayList selected)
    {
        this.selected = selected;
    }
    public getDelta(){

    }
    public void run() {
        try {
            logger.debug("Flushing tables now");
            if(System.getProperty("cassandra.username").isEmpty()) {
                if(System.getProperty("cassandra.port").isEmpty()) {
                    probe = new NodeProbe(System.getProperty("cassandra.address"));
                } else{
                    probe = new NodeProbe(System.getProperty("cassandra.address"),Integer.parseInt(System.getProperty("cassandra.port")));
                }
            }else{
                probe = new NodeProbe(System.getProperty("cassandra.address"),Integer.parseInt(System.getProperty("cassandra.port")),System.getProperty("cassandra.username"),System.getProperty("cassandra.password"));
            }
            Thread.sleep(1000);
            if(this.selected.size()==0) {
                new NodeTool.Flush().execute(probe);
                processData(System.getProperty("data_dir") + "data");
            }
            else{
                Map map = new HashMap();
                Map inner_map = new HashMap();
                ArrayList tables = new ArrayList();
                for(int i=0;i<this.selected.size();i++){
                    map = (Map)this.selected.get(i);
                    logger.debug(map.get("name").toString());
                    tables = map.get("tables")==null?null:(ArrayList) map.get("tables");
                    String[] table_names;
                    if(tables==null){
                        table_names=null;
                        probe.forceKeyspaceFlush(map.get("name").toString());
                    }else {
                        table_names = new String[tables.size()];
                        for (int j = 0; j < tables.size(); j++) {
                            table_names[j] = ((Map) tables.get(j)).get("name").toString();
                        }
                        probe.forceKeyspaceFlush(map.get("name").toString(), table_names);
                    }

                    processKeyspace(new File(System.getProperty("data_dir") + "data/" + map.get("name")), tables,table_names, false);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void processData(String data_dir)
    {
        File folder = new File(data_dir);
        File[] listOfKeyspaces = folder.listFiles();
        for (int i = 0; i < listOfKeyspaces.length; i++) {
            if (listOfKeyspaces[i].isDirectory()) {
                if(listOfKeyspaces[i].equals(null) || (((listOfKeyspaces[i].getName().equals("system") || listOfKeyspaces[i].getName().equals("system_traces"))) && System.getProperty("system").equals("false"))){
                    continue;
                }
                processKeyspace(listOfKeyspaces[i],null,null,false);
            }
        }
    }
    public void processKeyspace(File keyspace,ArrayList tables,String[] tables_names,boolean initialData)
    {
        File[] listofTables = keyspace.listFiles();
        if (tables != null) {
            for (int i = 0; i < listofTables.length; i++) {
                if (listofTables[i].equals(null)) {
                    continue;
                }
                for(int j=0;j<tables_names.length;j++) {
                    if(listofTables[i].getName().startsWith(tables_names[j])) {
                        logger.debug("Processing "+listofTables[i].getName());
                        ArrayList columns = ((Map)tables.get(j)).get("columns")==null?null:(ArrayList)((Map)tables.get(j)).get("columns");
                        processTable(listofTables[i],columns,initialData);
                    }
                }
            }
        }
        else{
            for (int i = 0; i < listofTables.length; i++) {
                if (listofTables[i].equals(null)) {
                    continue;
                }
                logger.debug("Processing "+listofTables[i].getName());
                processTable(listofTables[i],null,initialData);
            }
        }
    }
    public void processTable(File columnFamily,ArrayList columns,boolean initialData)
    {
        logger.debug(columnFamily.getName());
        File[] listoffiles = columnFamily.listFiles();
        if(listoffiles!=null) {
            for (int i = 0; i < listoffiles.length; i++) {
                if(!initialData) {
                    if (listoffiles[i].getName().equals("backups")) {
                        File[] files = listoffiles[i].listFiles();
                        for (int j = 0; j < files.length; j++) {
                            if (files[j].getName().endsWith("Data.db")) {
                                final File send = files[j];
                                final ArrayList send_columns = columns;
                                executor.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        processFile(send,send_columns);
                                    }
                                });
                            }
                        }
                        for (int j = 0; j < files.length; j++) {
                            files[j].delete();
                        }
                    }
                    if (listoffiles[i].getName().equals("snapshots")) {
                        final File send = listoffiles[i];
                        executor.execute(new Runnable() {
                            @Override
                            public void run() {
                                processTruncated(send);
                            }
                        });

                    }
                }
                else{
                    if(listoffiles[i].getName().endsWith("Data.db")){
                        processFile(listoffiles[i],columns);
                        logger.debug("done processing");
                    }
                }
            }
        }
    }
    public void processFile(File file,ArrayList columns)
    {
        Map json_ob = processSSTable.readSSTable(file.getAbsolutePath(), columns);
        indexDataES.indexIntoES(json_ob);
    }
    public void processTruncated(File folder) {
        for (int i = 0; i < folder.listFiles().length; i++) {
            File inner_folder = folder.listFiles()[i];
            File[] inner_files = inner_folder.listFiles();
            for (int j = 0; j < inner_files.length; j++) {
                if (inner_files[j].getName().endsWith("Data.db")) {
                    Map list_ob = processSSTable.readTruncatedData(inner_files[j].getAbsolutePath());
                    indexDataES.truncateFromES(list_ob);
                }
            }
        }
        FileUtils.deleteRecursive(folder);
    }
}