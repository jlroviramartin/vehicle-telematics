package master2018.flink.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class AverageSpeedEventSerializer extends Serializer<AverageSpeedEvent> {

    @Override
    public void write(Kryo kryo, Output output, AverageSpeedEvent t) {
        output.write(t.getTime1());
        output.write(t.getTime2());
        output.write(t.getVID());
        output.write(t.getHighway());
        output.write(t.getDirection());
        output.writeDouble(t.getAverageSpeed());
    }

    @Override
    public AverageSpeedEvent read(Kryo kryo, Input input, Class<AverageSpeedEvent> type) {
        return new AverageSpeedEvent(
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readByte(),
                input.readDouble());
    }
}
