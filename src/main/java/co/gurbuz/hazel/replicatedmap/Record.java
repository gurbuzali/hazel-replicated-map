package co.gurbuz.hazel.replicatedmap;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @ali 09/11/13
 */
public class Record<V> implements IdentifiedDataSerializable {

    private long version;

    private V value;

    public Record(){

    }

    public Record(long version, V value) {
        this.version = version;
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public V getValue() {
        return value;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record)) return false;

        Record record = (Record) o;

        if (version != record.version) return false;
        if (!value.equals(record.value)) return false;

        return true;
    }

    public int hashCode() {
        int result = (int) (version ^ (version >>> 32));
        result = 31 * result + value.hashCode();
        return result;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder("Record{");
        sb.append("version=").append(version);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public int getFactoryId() {
        return SerializerHook.F_ID;
    }

    public int getId() {
        return SerializerHook.RECORD;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(value);
        out.writeLong(version);
    }

    public void readData(ObjectDataInput in) throws IOException {
        value = in.readObject();
        version = in.readLong();
    }
}
