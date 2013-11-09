package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

/**
 * @ali 09/11/13
 */
public class ReplicatedMapProxy extends AbstractDistributedObject<ReplicatedMapService> implements ReplicatedMap {

    String name;

    public ReplicatedMapProxy(String name, NodeEngine nodeEngine, ReplicatedMapService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }
}
