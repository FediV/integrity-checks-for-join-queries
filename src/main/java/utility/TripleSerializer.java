package utility;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;

/**
 * The TripleSerializer class.
 * Custom Apache Commons' Triple serializer for the CustomJsonParser.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@SuppressWarnings("rawtypes")
public final class TripleSerializer extends JsonSerializer<Triple> {
    @Override
    public void serialize(Triple value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeObjectField("left", value.getLeft());
        gen.writeObjectField("middle", value.getMiddle());
        gen.writeObjectField("right", value.getRight());
        gen.writeEndObject();
    }
}