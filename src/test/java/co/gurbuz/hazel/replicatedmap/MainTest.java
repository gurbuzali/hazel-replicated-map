package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * @ali 09/11/13
 */
public class MainTest {


    public static void main(String[] args) {
        final ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true);
        serviceConfig.setClassName(ReplicatedMapService.class.getName());
        serviceConfig.setName(ReplicatedMapService.SERVICE_NAME);
    
        final Config config = new Config();
        final ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(serviceConfig);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        ReplicatedMap rm = instance.getDistributedObject(ReplicatedMapService.SERVICE_NAME, "rm");
        System.err.println("rm: " + rm);

    }
    
    


}
