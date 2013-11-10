package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.core.DistributedObject;

/**
 * @ali 09/11/13
 */
public interface ReplicatedMap<K, V> extends DistributedObject {

    public V put(K key, V value);

    public V get(K key);

    public V remove(K key);

}
