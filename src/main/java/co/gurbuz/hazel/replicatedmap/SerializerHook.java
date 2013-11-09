package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * @ali 09/11/13
 */
public class SerializerHook implements DataSerializerHook {

    static final int F_ID = FactoryIdHelper.getFactoryId("hazelcast.serialization.replicated.map", -82);

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId){
                }
                return null;
            }
        };
    }
}
