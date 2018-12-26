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

/**
 * This class contains the logic of selecting the tables required to be read.
 */
public class ProcessCassandraData extends TimerTask{
    private static final Logger logger = LoggerFactory.getLogger(ProcessCassandraData.class);
    private ArrayList keyspaces_selected = new ArrayList();
    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    final static String DATA_DB = "Data.db";
    final static String DATA = "data";
    final static String NAME = "name";
    final static String TABLES = "tables";
    final static String COLUMNS = "columns";
    final static String SNAPSHOTS = "snapshots";
    static NodeProbe probe;
    public ProcessCassandraData(){
        this.keyspaces_selected = ReadConfig.keyspaces_selected;//Copies the keyspace selection data structure locally.
    }
    public void run() {//Method which is called periodically.
        try {
            logger.debug("Flushing tables now");
            //Connecting to cassandra,to flush data
            if(ReadConfig.getCassandraUsername().isEmpty()) {
                if(ReadConfig.getCassandraPort().isEmpty()) {
                    probe = new NodeProbe(ReadConfig.getCassandraAddress());
                } else{
                    probe = new NodeProbe(ReadConfig.getCassandraAddress(),Integer.parseInt(ReadConfig.getCassandraPort()));
                }
            }else{
                probe = new NodeProbe(ReadConfig.getCassandraAddress(),Integer.parseInt(ReadConfig.getCassandraPort()),ReadConfig.getCassandraUsername(),ReadConfig.getCassandraPassword());
            }
            //If no keyspace is explicity selected,process all keyspaces.
            if(this.keyspaces_selected.size()==0) {
                new NodeTool.Flush().execute(probe);
                Thread.sleep(1000);//To provide for latency of creation of the backup data
                processAllData(ReadConfig.getCassandraDataDir() + DATA,false);
            }
            else{//Process selected keyspaces
                Map map = new HashMap();
                ArrayList tables = new ArrayList();
                for(int i=0;i<this.keyspaces_selected.size();i++){
                    map = (Map)this.keyspaces_selected.get(i);
                    logger.debug(map.get(NAME).toString());
                    tables = map.get(TABLES)==null?null:(ArrayList) map.get(TABLES);
                    String[] table_names;
                    if(tables==null){
                        table_names=null;
                        //Forcing a flush of memtables at the intervals for all tables of the keyspace.
                        probe.forceKeyspaceFlush(map.get(NAME).toString());
                    }else {
                        table_names = new String[tables.size()];
                        for (int j = 0; j < tables.size(); j++) {
                            table_names[j] = ((Map) tables.get(j)).get(NAME).toString();
                        }
                        //Forcing a flush for only the selected tables.
                        probe.forceKeyspaceFlush(map.get(NAME).toString(), table_names);
                    }
                    Thread.sleep(1000);//To provide for latency of creation of the backup data
                    processKeyspace(new File(ReadConfig.getCassandraDataDir() + DATA +"/" + map.get(NAME)), tables,table_names, false);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    //Process all keyspaces except system keyspaces until explicity declared to be processed.
    public void processAllData(String data_dir,boolean initData)
    {
        File folder = new File(data_dir);
        File[] listOfKeyspaces = folder.listFiles();
        for (int i = 0; i < listOfKeyspaces.length; i++) {
            if (listOfKeyspaces[i].isDirectory()) {
                if(listOfKeyspaces[i].equals(null) || (((listOfKeyspaces[i].getName().equals("system") || listOfKeyspaces[i].getName().equals("system_traces"))) && ReadConfig.getSystemTablesRequirement().equals("false"))){
                    continue;
                }
                processKeyspace(listOfKeyspaces[i],null,null,initData);
            }
        }
    }
    //Method to process individual keyspaces.
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
                        //identifying all explicity selected columns of the table.
                        ArrayList columns = ((Map)tables.get(j)).get(COLUMNS)==null?null:(ArrayList)((Map)tables.get(j)).get(COLUMNS);
                        processTable(listofTables[j],columns,initialData);
                    }
                }
            }
        }
        else{//If no table is explicity selected.
            for (int i = 0; i < listofTables.length; i++) {
                if (listofTables[i].equals(null)) {
                    continue;
                }
                logger.debug("Processing "+listofTables[i].getName());
                processTable(listofTables[i],null,initialData);
            }
        }
    }
    //Method to process individual table selected.
    public void processTable(File columnFamily,ArrayList columns,boolean initialData)
    {
        logger.debug(columnFamily.getName());
        File[] listoffiles = columnFamily.listFiles();
        if(listoffiles!=null) {
        if(!initialData) {
            //Processing all incremental back ups
            for (int i = 0; i < listoffiles.length; i++) {
                if (listoffiles[i].getName().equals("backups")) {
                    File[] files = listoffiles[i].listFiles();
                    for (int j = 0; j < files.length; j++) {
                        if (files[j].getName().endsWith(DATA_DB)) {
                            processFile(files[j], columns);
                        }
                    }
                    FileUtils.deleteRecursive(listoffiles[i]);
                }
                //While processing backup ,if there is truncated data present,process it.
                if (listoffiles[i].getName().equals(SNAPSHOTS)) {
                    processTruncated(listoffiles[i]);
                }
            }
        }else{
            //If processing intial data, go to snapshots folder and look for data stored there.
            File init = new File(columnFamily.getAbsolutePath()+"/"+SNAPSHOTS);
            File[] init_files = init.listFiles()[0].listFiles();
            if(init_files!=null)
            for(int i=0;i<init_files.length;i++){
                if (init_files[i].getName().endsWith(DATA_DB)) {
                    processFile(init_files[i], columns);
                }
            }
            FileUtils.deleteRecursive(init);//Delete all backups once processed.
            }
        }
    }
    //To process individual files
    public void processFile(File file,ArrayList columns)
    {
        Map json_ob = ProcessSSTable.readSSTable(file.getAbsolutePath(), columns);
        IndexDataES.indexIntoES(json_ob);
    }
    //To process truncated data
    public void processTruncated(File folder) {
        File[] list_files = folder.listFiles();
        if(list_files!=null) {
            for (int i = 0; i < list_files.length; i++) {
                File inner_folder = list_files[i];
                File[] inner_files = inner_folder.listFiles();
                if(inner_files!=null) {
                    for (int j = 0; j < inner_files.length; j++) {
                        if (inner_files[j].getName().endsWith(DATA_DB)) {
                            Map list_ob = ProcessSSTable.readTruncatedData(inner_files[j].getAbsolutePath());
                            IndexDataES.truncateFromES(list_ob);
                        }
                    }
                }
            }
        }
        FileUtils.deleteRecursive(folder);
    }
}