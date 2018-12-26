/**
 * Created by sraghavendra on 09/03/16.
 */
package CassandraToES;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import org.apache.cassandra.tools.NodeProbe;

/**
 * This class takes the snapshot of the keyspaces selected for the initial data already present in the database.
 * Hence it doesn't get affected by the compaction policy of cassandra.
 */
public class InitialData {
    private static final Logger logger = LoggerFactory.getLogger(ProcessCassandraData.class);
    static ArrayList keyspaces_selected = new ArrayList();
    final static String DATA_DB = "Data.db";
    final static String DATA = "data";
    final static String NAME = "name";
    final static String TABLES = "tables";
    final static String COLUMNS = "columns";

    //NodeProbe to connect to the cassandra instance
    static NodeProbe probe;

    public static void main(String []args){
        String config_file_name;
        if(args.length==0) {
            config_file_name = "";
        }else{
            config_file_name = args[0];
        }
        //Read Configurations file and extract the details
        new ReadConfig(config_file_name);
        //Setup actual connection to cassandra
        try {
            if(ReadConfig.getCassandraUsername().isEmpty()) {
                if(ReadConfig.getCassandraPort().isEmpty()) {
                    probe = new NodeProbe(ReadConfig.getCassandraAddress());
                } else{
                    probe = new NodeProbe(ReadConfig.getCassandraAddress(),Integer.parseInt(ReadConfig.getCassandraPort()));
                }
            }else{
                probe = new NodeProbe(ReadConfig.getCassandraAddress(),Integer.parseInt(ReadConfig.getCassandraPort()),ReadConfig.getCassandraUsername(),ReadConfig.getCassandraPassword());
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        keyspaces_selected = ReadConfig.keyspaces_selected;
        //Callinf function which does the processing of the data
        processInitialData();
        logger.debug("done");
        System.exit(0);
    }
    public static void processInitialData() {
        try {
            ProcessCassandraData methods = new ProcessCassandraData();//Object to select proper files
            //if no keyspace was explicity selected,all keyspaces are taken into consideration.
            if (keyspaces_selected == null || keyspaces_selected.size() == 0) {
                String[] keyspaces = new String[probe.getKeyspaces().size()];
                probe.getKeyspaces().toArray(keyspaces);
                probe.takeSnapshot("All_keyspaces",null,keyspaces);
                methods.processAllData(ReadConfig.getCassandraDataDir() + DATA,true);
            } else {//If keyspaces were explicity selected
                Map map;//to process the selection in terms of a map
                ArrayList tables;//to access tables information of each keyspace selected.
                logger.debug("processing selected keyspaces");
                for (int i = 0; i < keyspaces_selected.size(); i++) {
                    //For each selected keyspace,first take a snapshot the existing data.
                    map = (Map) keyspaces_selected.get(i);
                    File keyspace = new File(ReadConfig.getCassandraDataDir() + DATA+"/" + map.get(NAME));
                    //Check for selection of tables in the keyspace
                    tables = map.get(TABLES) == null ? null : (ArrayList) map.get(TABLES);
                    String[] table_names;
                    String[] for_snapshot;
                    if (tables == null) {//If no table is selected,take snapshot of the whole document.
                        table_names = null;
                        probe.takeSnapshot(keyspace.getName(),null,keyspace.getName());
                    } else {//Else take snapshot of only the selected tables
                        table_names = new String[tables.size()];
                        for_snapshot = new String[tables.size()];
                        for (int j = 0; j < tables.size(); j++) {
                            table_names[j] = ((Map) tables.get(j)).get(NAME).toString();
                            for_snapshot[j] = keyspace.getName() + "." + table_names[j];//To supply the data in the format required by the library method.
                        }
                        probe.takeMultipleColumnFamilySnapshot(keyspace.getName(), for_snapshot);
                    }
                    methods.processKeyspace(keyspace, tables, table_names, true);//Once the data is available,process them to index them.
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}