package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;

/**
 * @ali 09/11/13
 */
public class PutOperation extends AbstractNamedOperation implements IdentifiedDataSerializable{

    Data keyData;
    Data valueData;
    long version;
    transient Record response;

    public PutOperation() {
    }

    public PutOperation(String name, Data keyData, Data valueData, long version) {
        super(name);
        this.keyData = keyData;
        this.valueData = valueData;
        this.version = version;
    }

    public void run() throws Exception {
        final ReplicatedMapService service = getService();
        final MapContainer container = service.getOrCreateMapContainer(name);
        final Object key = getNodeEngine().toObject(keyData);
        final Object value = getNodeEngine().toObject(valueData);
        response = container.put(key, value, version);
    }

    public Object getResponse() {
        return response;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        keyData.writeData(out);
        valueData.writeData(out);
        out.writeLong(version);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        keyData = new Data();
        keyData.readData(in);
        valueData = new Data();
        valueData.readData(in);
        version = in.readLong();
    }

    public int getFactoryId() {
        return SerializerHook.F_ID;
    }

    public int getId() {
        return SerializerHook.PUT;
    }
}
