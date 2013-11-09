package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;

/**
 * @ali 09/11/13
 */
public class ReplicatedMapService implements RemoteService {

    public static final String SERVICE_NAME = "grbz:replicatedMapService";

    private final NodeEngine nodeEngine;

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public DistributedObject createDistributedObject(String objectName) {
        return new ReplicatedMapProxy(objectName, nodeEngine, this);
    }

    public void destroyDistributedObject(String objectName) {
        //TODO destroy
    }
}
