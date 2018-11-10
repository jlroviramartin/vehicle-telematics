package master2018.flink.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class PrincipalEventSerializer extends Serializer<PrincipalEvent> {

    @Override
    public void write(Kryo kryo, Output output, PrincipalEvent t) {
        output.write(t.getTime());
        output.write(t.getVid());
        output.write(t.getSpeed());
        output.write(t.getLane());
        output.write(t.getHighway());
        output.write(t.getDirection());
        output.write(t.getSegment());
        output.write(t.getPosition());
    }

    @Override
    public PrincipalEvent read(Kryo kryo, Input input, Class<PrincipalEvent> type) {
        return new PrincipalEvent(
                input.readInt(),
                input.readInt(),
                input.readByte(),
                input.readInt(),
                input.readByte(),
                input.readByte(),
                input.readByte(),
                input.readInt());
    }

}
