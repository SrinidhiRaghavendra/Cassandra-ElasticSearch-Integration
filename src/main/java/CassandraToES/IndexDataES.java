/**
 * Created by sraghavendra on 02/03/16.
 */
package CassandraToES;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

/**
 * Class to talk to ES and synchronise it with the state of cassandra cluster.
 */
public class IndexDataES {
    private static final Logger logger = LoggerFactory.getLogger(IndexDataES.class);
    private static TransportClient client = TransportClient.builder().settings(Settings.settingsBuilder().put("cluster.name", ReadConfig.getESClustername()).put("client.transport.sniff", false).put("client.transport.ping_timeout", 20, TimeUnit.SECONDS).build()).build().addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(ReadConfig.getESAddress(),Integer.parseInt(ReadConfig.getESPort()))));
    final static String INDEX_NAME = "index_name";
    final static String TYPE_NAME = "type_name";
    //Method which recieves all data present in a table to be indexed.
    public static void indexIntoES(Map data)
    {
        //Meta data for ES purpose to index.
        String index_name = (String) data.get(INDEX_NAME);
        String type_name = (String) data.get(TYPE_NAME);

        try {//Check if index already exists,elase create one.
            if(!client.admin().indices().prepareExists(index_name).execute().get().isExists()){
                client.admin().indices().prepareCreate(index_name).execute();
                Thread.sleep(2000);//To accomodate the latency of creation.
            }
            Iterator entries = data.entrySet().iterator();//Iterate of the all the data recieved.
            while (entries.hasNext()) {
                Map.Entry entry = (Map.Entry) entries.next();
                String key = (String) entry.getKey();
                Object value = entry.getValue();
                if(key.equals(INDEX_NAME) || key.equals(TYPE_NAME)){//already processed.
                    continue;
                }
                if(value instanceof Map) {
                    //Check if document already present in ES,else index the new document.
                    if (!client.prepareGet(index_name, type_name, key).execute().get().isExists()) {
                        logger.debug(client.prepareIndex(index_name, type_name, key).setSource((Map) value).execute().get().getId());
                    } else {
                        Iterator ind_entries = ((Map) value).entrySet().iterator();
                        while (ind_entries.hasNext()) {
                            Map.Entry ind_entry = (Map.Entry) ind_entries.next();
                            String ind_key = (String) ind_entry.getKey();
                            Object ind_value = ind_entry.getValue();
                            //Check if any data is deleted, delete appropriate fields.
                            if (!ind_value.equals("deleted")) {
                                logger.debug(client.prepareUpdate(index_name, type_name, key).setDoc(ind_key, ind_value).execute().get().getId());
                            } else {//update other fields.
                                logger.debug(client.prepareUpdate(index_name, type_name, key).setDoc(ind_key, "").execute().get().getId());
                            }
                        }
                    }
                } else if (value instanceof String) {//If a whole row is deleted,delete the document from the index.
                    logger.debug(client.prepareDelete(index_name, type_name, key).execute().get().getId());
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    //Delete truncated data from ES.
    public static void truncateFromES(Map data){
        List ids = (List) data.get("data");
        for (Object key:ids) {//Delete all the keys from that particular type.
            logger.debug(client.prepareDelete(data.get(INDEX_NAME).toString(), data.get(TYPE_NAME).toString(), key.toString()).execute().toString());
        }
    }
}
