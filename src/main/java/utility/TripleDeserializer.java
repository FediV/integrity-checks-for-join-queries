package utility;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;

/**
 * The TripleDeserializer class.
 * Custom Apache Commons' Triple deserializer for the CustomJsonParser.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class TripleDeserializer extends JsonDeserializer<Triple<?,?,?>> {
    @Override
    public Triple<?,?,?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        p.nextValue();
        String left = p.getText();
        p.nextToken();
        p.nextToken();
        String middle = p.getText();
        p.nextToken();
        p.nextToken();
        String right = p.getText();
        p.nextToken();
        return Triple.of(left, middle, right);
    }
}
