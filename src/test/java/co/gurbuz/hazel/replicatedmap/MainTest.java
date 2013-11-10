package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @ali 09/11/13
 */
public class MainTest {

    static {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");

        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);

    }


    public static void main(String[] args) throws IOException, InterruptedException {
        final ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true);
        serviceConfig.setClassName(ReplicatedMapService.class.getName());
        serviceConfig.setName(ReplicatedMapService.SERVICE_NAME);

        final Config config = new Config();
        config.getGroupConfig();

        final ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(serviceConfig);
        config.getSerializationConfig().addDataSerializableFactory(SerializerHook.F_ID, new SerializerHook().createFactory());
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        final ReplicatedMap<String, String> rm1 = instance1.getDistributedObject(ReplicatedMapService.SERVICE_NAME, "rm");
        final ReplicatedMap<String, String> rm2 = instance2.getDistributedObject(ReplicatedMapService.SERVICE_NAME, "rm");
        final ReplicatedMap<String, String> rm3 = instance3.getDistributedObject(ReplicatedMapService.SERVICE_NAME, "rm");

        int threadCount = 5;
        final AtomicBoolean running = new AtomicBoolean(true);

        final CountDownLatch latch = new CountDownLatch(3 * threadCount);
        for (int i = 0; i < threadCount; i++) {
            new Thread(){
                public void run() {
                    final Random random = new Random(System.currentTimeMillis());
                    while (running.get()) {
                        final int key = random.nextInt(100);
                        rm1.put("key"+key, "instance1-" + random.nextLong());
                    }
                    latch.countDown();
                }
            }.start();
        }

        for (int i = 0; i < threadCount; i++) {
            new Thread(){
                public void run() {
                    final Random random = new Random(System.currentTimeMillis());
                    while (running.get()) {
                        final int key = random.nextInt(100);
                        rm2.put("key"+key, "instance2-" + random.nextLong());
                    }
                    latch.countDown();
                }
            }.start();
        }

        for (int i = 0; i < threadCount; i++) {
            new Thread(){
                public void run() {
                    final Random random = new Random(System.currentTimeMillis());
                    while (running.get()) {
                        final int key = random.nextInt(100);
                        rm3.put("key"+key, "instance3-" + random.nextLong());
                    }
                    latch.countDown();
                }
            }.start();
        }


        System.err.println("asdf sleeping");
        Thread.sleep(20000);

        running.set(false);

        latch.await();
        System.err.println("asdf sleeping");
        Thread.sleep(2000);


        for (int j = 0; j<100; j++){
            String firstVal = rm1.get("key"+j);
            String secondVal = rm2.get("key"+j);
            String thirdVal = rm3.get("key"+j);

            if (!firstVal.equals(secondVal) || !secondVal.equals(thirdVal)) {
                System.err.println("severe not equal " + firstVal + " " + secondVal + " " + thirdVal);
            } else {
                System.err.println("WARN equal " + firstVal + " " + secondVal + " " + thirdVal);
            }
        }

        System.err.println("asdf done!");


    }


}
