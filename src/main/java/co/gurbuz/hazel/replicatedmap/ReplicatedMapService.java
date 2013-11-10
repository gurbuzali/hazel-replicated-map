package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @ali 09/11/13
 */
public class ReplicatedMapService implements RemoteService {

    public static final String SERVICE_NAME = "grbz:replicatedMapService";

    public static final String VERSION_PREFIX = "grbz:version:";

    private final NodeEngine nodeEngine;

    private final ConcurrentMap<String, MapContainer> containerMap = new ConcurrentHashMap<String, MapContainer>();

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public DistributedObject createDistributedObject(String objectName) {
        return new ReplicatedMapProxy(objectName, nodeEngine, this);
    }

    public void destroyDistributedObject(String objectName) {
        //TODO destroy
    }

    public long getVersion(String name, Object key){
        return nodeEngine.getHazelcastInstance().getAtomicLong(VERSION_PREFIX+name+key.toString()).incrementAndGet();
    }

    public <K, V> MapContainer<K, V> getOrCreateMapContainer(String name){
        MapContainer container = containerMap.get(name);
        if (container == null){
            container = new MapContainer(name, nodeEngine, this);
            final MapContainer current = containerMap.putIfAbsent(name, container);
            container = current == null ? container : current;
        }
        return container;
    }
}
