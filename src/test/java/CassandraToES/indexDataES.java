/**
 * Created by sraghavendra on 02/03/16.
 */
package CassandraToES;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import org.json.JSONObject;

public class indexDataES {
    private static final Logger logger = LoggerFactory.getLogger(indexDataES.class);
    private static TransportClient client = TransportClient.builder().settings(Settings.settingsBuilder().put("cluster.name", System.getProperty("cluster.name")).put("client.transport.sniff", false).put("client.transport.ping_timeout", 20, TimeUnit.SECONDS).build()).build().addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(System.getProperty("ES-address"),Integer.parseInt(System.getProperty("ES-port")))));
    public static void indexIntoES(Map data)
    {
        String index_name = (String) data.get("index_name");
        String type_name = (String) data.get("type_name");

        try {
            if(!client.admin().indices().prepareExists(index_name).execute().get().isExists()){
                client.admin().indices().prepareCreate(index_name).execute();
                Thread.sleep(2000);
            }
            Iterator entries = data.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry entry = (Map.Entry) entries.next();
                String key = (String) entry.getKey();
                Object value = entry.getValue();
                if(key.equals("index_name") || key.equals("type_name")){
                    continue;
                }
                if(value instanceof Map) {
                    if (!client.prepareGet(index_name, type_name, key).execute().get().isExists()) {
                        logger.debug(client.prepareIndex(index_name, type_name, key).setSource((Map) value).execute().get().getId());
                    } else {
                        Iterator ind_entries = ((Map) value).entrySet().iterator();
                        while (ind_entries.hasNext()) {
                            Map.Entry ind_entry = (Map.Entry) ind_entries.next();
                            String ind_key = (String) ind_entry.getKey();
                            Object ind_value = ind_entry.getValue();

                            if (!ind_value.equals("deleted")) {
                                logger.debug(client.prepareUpdate(index_name, type_name, key).setDoc(ind_key, ind_value).execute().get().getId());
                            } else {
                                logger.debug(client.prepareUpdate(index_name, type_name, key).setDoc(ind_key, "").execute().get().getId());
                            }
                        }
                    }
                } else if (value instanceof String) {
                    logger.debug(client.prepareDelete(index_name, type_name, key).execute().get().getId());
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void truncateFromES(Map data){
        List ids = (List) data.get("data");
        for (Object key:ids) {
            logger.debug(client.prepareDelete(data.get("index_name").toString(), data.get("type_name").toString(), key.toString()).execute().toString());
        }
    }
}
