package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 09/11/13
 */
public class MapContainer<K, V> {

    private final String name;

    private final NodeEngine nodeEngine;

    private final ReplicatedMapService service;

    private final ConcurrentMap<K, Record<V>> recordMap = new ConcurrentHashMap<K, Record<V>>();

    public MapContainer(String name, NodeEngine nodeEngine, ReplicatedMapService service) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.service = service;
    }

    public Record<V> put(K key, V value, long version) {
        final Record<V> current = recordMap.get(key);
        if (current != null){
            if (current.getVersion() >= version) {
                return current;
            } else {
                if(recordMap.replace(key, current, new Record<V>(version, value))){
                    return null;
                } else {
                    return put(key, value, version);
                }
            }
        }
        final Record<V> ret = recordMap.putIfAbsent(key, new Record<V>(version, value));
        if (ret == null){
            return null;
        }
        return put(key, value, version);
    }

    public V get(K key){
        final Record<V> record = recordMap.get(key);
        return record == null ? null : record.getValue();
    }

}
