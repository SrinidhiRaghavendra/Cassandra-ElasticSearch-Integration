/**
 * Created by sraghavendra on 29/02/16.
 */
package CassandraToES;

import java.util.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletedCell;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableReader.Operator;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.json.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class processSSTable {
    private static final Logger logger = LoggerFactory.getLogger(processSSTable.class);
    public static Map readSSTable(String filename,ArrayList columns)
    {
        Map<String, Object> json_row = new HashMap<String, Object>();
        try {
            Descriptor descriptor = Descriptor.fromFilename(filename);

            json_row.put("index_name",descriptor.ksname);
            json_row.put("type_name",descriptor.cfname);

            SSTableReader reader = SSTableReader.open(descriptor);
            ISSTableScanner scanner = reader.getScanner();
            int i =1;
            while(scanner.hasNext()) {
                SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                CFMetaData metadata = row.getColumnFamily().metadata();
                Map<String, Object> json_ex = new HashMap<String, Object>();
                JSONObject json_xml;
                if(row.hasNext()){
                while(row.hasNext()) {
                    OnDiskAtom test = row.next();
                    if(test instanceof RangeTombstone){
                        continue;
                    }
                    CellNameType comparator = metadata.comparator;
                    AbstractType validator = metadata.getValueValidator(((Cell) test).name());
                    String name = comparator.getString(((Cell) test).name());
                    if(name.isEmpty() || (columns!=null && !columns.contains(name)))
                        continue;
                    if(test instanceof DeletedCell) {
                        if(name.endsWith(".xml")) {
                            String[] med = name.split(":");
                            String[] inner_med = med[med.length-1].split("@");
                            String realname = inner_med[inner_med.length-1].split("\\.")[0];
                            json_ex.put(realname,"deleted");
                        }
                    }else if (test instanceof Cell) {
                        AbstractType type = validator.asCQL3Type().getType();
                        String value;
                        if (type instanceof BytesType) {
                            value = new String(((Cell) test).value().array(), "UTF8");
                        } else {
                            value = validator.getString(((Cell) test).value());
                        }
                        if(name.endsWith(".xml")) {
                            String[] med = name.split(":");
                            String[] inner_med = med[med.length-1].split("@");
                            String realname = inner_med[inner_med.length-1].split("\\.")[0];
                            json_xml = XML.toJSONObject(value);
                            json_ex.put(realname,json_xml.toString());
                        }
                        else {
                            json_ex.put(name,value);
                        }
                    }else if(test instanceof ExpiringCell) {
                        logger.debug(Integer.valueOf(((ExpiringCell)test).getTimeToLive()).toString());
                        logger.debug(Integer.valueOf(test.getLocalDeletionTime()).toString());
                    }
                    else if(test instanceof CounterCell){
                        logger.debug(Long.valueOf(((CounterCell)test).total()).toString());
                    }
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
