/**
 * Created by sraghavendra on 29/02/16.
 */
package CassandraToES;

import java.util.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.DeletedCell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to read the selected SSTables and create a JSON object to index in ES.
 */
public class ProcessSSTable {
    private static final Logger logger = LoggerFactory.getLogger(ProcessCassandraData.class);
    //Function to read all the sstables selected.
    public static Map readSSTable(String filename,ArrayList columns)
    {
        Map<String, Object> json_row = new HashMap<String, Object>();
        try {
            //A descriptor object for the table,which reads all associated data about the table.
            Descriptor descriptor = Descriptor.fromFilename(filename);

            //Meta data about the table connected.
            json_row.put("index_name",descriptor.ksname);
            json_row.put("type_name",descriptor.cfname);
            //A reader and scanner for the descriptor of the table.
            SSTableReader reader = SSTableReader.open(descriptor);
            ISSTableScanner scanner = reader.getScanner();
            int i =1;
            //Loop over all the entries in the table.
            while(scanner.hasNext()) {
                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                CFMetaData metadata = row.getColumnFamily().metadata();
                Map<String, Object> json_ex = new HashMap<String, Object>();
                JSONObject json_xml;
                if(row.hasNext()){
                while(row.hasNext()) {
                    OnDiskAtom test = row.next();
                    if(test instanceof RangeTombstone){
                        //Ignore if deleted data.
                        continue;
                    }
                    CellNameType comparator = metadata.comparator;//Comparator to process the column key.
                    AbstractType validator = metadata.getValueValidator(((Cell) test).name());//validator to process the column value.
                    String name = comparator.getString(((Cell) test).name());
                    //Check for boundary conditions.
                    if(name.isEmpty() || (columns!=null && !columns.contains(name)))
                        continue;
                    if(test instanceof DeletedCell) {
                        //identifying the the deleted data and marking it as deleted.
                        if(name.endsWith(".xml")) {
                            String[] med = name.split(ReadConfig.COLON);
                            String[] inner_med = med[med.length-1].split(ReadConfig.AT);
                            String realname = inner_med[inner_med.length-1].split(ReadConfig.DOT)[0];
                            json_ex.put(realname,"deleted");
                        }
                        else{
                            json_ex.put(name,"deleted");
                        }
                    }else if (test instanceof Cell) {
                        //extracting inserted or updated data.
                        AbstractType type = validator.asCQL3Type().getType();
                        String value;
                        if (type instanceof BytesType) {
                            //COnverting bytes to utf8 format for storing human-understandable data in ES.
                            value = new String(((Cell) test).value().array(), "UTF8");
                        } else {
                            value = validator.getString(((Cell) test).value());
                        }
                        //Special processing for xml files
                        if(name.endsWith(".xml")) {
                            String[] med = name.split(ReadConfig.COLON);
                            String[] inner_med = med[med.length-1].split(ReadConfig.AT);
                            String realname = inner_med[inner_med.length-1].split(ReadConfig.DOT)[0];
                            json_xml = XML.toJSONObject(value);
                            json_ex.put(realname,json_xml.toString());
                        }
                        else {
                            json_ex.put(name,value);
                        }
                    }else if(test instanceof ExpiringCell) {//Handling other types of cells
                        logger.debug(Integer.valueOf(((ExpiringCell)test).getTimeToLive()).toString());
                        logger.debug(Integer.valueOf(test.getLocalDeletionTime()).toString());
                    }
                    else if(test instanceof CounterCell){
                        logger.debug(Long.valueOf(((CounterCell)test).total()).toString());
                    }
                    //Finally creating the data to be forwarded for indexing.
                    json_row.put(new String(row.getKey().getKey().array(),"UTF8"),json_ex);
                }
                }
                else{
                    json_row.put(new String(row.getKey().getKey().array(),"UTF8"),"deleted");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return json_row;
    }
    //Identify all the row keys to be deleted.
    public static Map readTruncatedData(String filename)
    {
        Map<String, Object> json_row = new HashMap<String, Object>();
        List<String> row_keys = new ArrayList<String>();
        try {
            Descriptor descriptor = Descriptor.fromFilename(filename);
            json_row.put("index_name",descriptor.ksname);
            json_row.put("type_name",descriptor.cfname);


            SSTableReader reader = SSTableReader.open(descriptor);
            ISSTableScanner scanner = reader.getScanner();

            while(scanner.hasNext()) {
                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                row_keys.add(new String(row.getKey().getKey().array(),"UTF8"));
            }
            json_row.put("data",row_keys);
        }catch(Exception e) {
            e.printStackTrace();
        }
        return json_row;
    }
}
