package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * @ali 09/11/13
 */
public class ReplicatedMapProxy<K, V> extends AbstractDistributedObject<ReplicatedMapService> implements ReplicatedMap<K, V> {

    String name;

    public ReplicatedMapProxy(String name, NodeEngine nodeEngine, ReplicatedMapService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    public V put(K key, V value) {
        long version = getService().getVersion(name, key);
        V result = localPut(key, value, version);
        if (result == null){
            return putAll(key, value, version);
        }
        return result;
    }

    private V localPut(K key, V value, long version){
        final MapContainer<K, V> container = getService().getOrCreateMapContainer(name);
        final Record<V> record = container.put(key, value, version);
        return record == null ? null : record.getValue();
    }

    private V putAll(K key, V value, long version) {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            final ClusterService clusterService = nodeEngine.getClusterService();
            final Collection<MemberImpl> members = new ArrayList<MemberImpl>(clusterService.getMemberList());
            if (members.size() < 2) {
                return value;
            }
            final Data keyData = nodeEngine.toData(key);
            final Data valueData = nodeEngine.toData(value);
            final ArrayList<Future<Record<V>>> futures = new ArrayList<Future<Record<V>>>(members.size()-1);
            for (MemberImpl member : members) {
                if (member.localMember()){
                    continue;
                }
                final PutOperation operation = new PutOperation(name, keyData, valueData, version);
                Invocation invocation = nodeEngine.getOperationService().
                        createInvocationBuilder(ReplicatedMapService.SERVICE_NAME, operation, member.getAddress()).build();
                futures.add(invocation.invoke());
            }
            Record<V> record = null;
            for (Future<Record<V>> future : futures) {
                final Record<V> rec = future.get();
                if (rec != null) {
                    if (record == null) {
                        record = rec;
                    } else if (rec.getVersion() > record.getVersion()) {
                        record = rec;
                    }
                }
            }
            if (record == null){
                return value;
            }
            return localPut(key, record.getValue(), record.getVersion());
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public V remove(K key) {
        return null;
    }

    public V get(K key) {
        final MapContainer<K, V> container = getService().getOrCreateMapContainer(name);
        return container.get(key);
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }


}
