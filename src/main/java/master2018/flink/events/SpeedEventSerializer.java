package master2018.flink.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class SpeedEventSerializer extends Serializer<SpeedEvent> {

    @Override
    public void write(Kryo kryo, Output output, SpeedEvent t) {
        output.write(t.getTime());
        output.write(t.getVid());
        output.write(t.getHighway());
        output.write(t.getSegment());
        output.write(t.getDirection());
        output.write(t.getSpeed());
    }

    @Override
    public SpeedEvent read(Kryo kryo, Input input, Class<SpeedEvent> type) {
        return new SpeedEvent(
                input.readInt(),
                input.readInt(),
                input.readInt(),
                input.readByte(),
                input.readByte(),
                input.readByte());
    }

}
