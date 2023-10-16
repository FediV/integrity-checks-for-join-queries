package utility;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/**
 * The CustomJsonParser generic class.
 * A custom JSON file parser based on Jackson.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class CustomJsonParser<T> {
    private static final ObjectMapper mapper;
    private final Class<T> objectClass;
    private final String jsonFilePath;

    static {
        mapper = new ObjectMapper();

        mapper.registerModule(new SimpleModule().addSerializer(Pair.class, new PairSerializer()));
        mapper.registerModule(new SimpleModule().addDeserializer(Pair.class, new PairDeserializer()));

        mapper.registerModule(new SimpleModule().addSerializer(Triple.class, new TripleSerializer()));
        mapper.registerModule(new SimpleModule().addDeserializer(Triple.class, new TripleDeserializer()));
    }

    public CustomJsonParser(String jsonFilePath, Class<T> objectClass) {
        this.objectClass = objectClass;
        this.jsonFilePath = jsonFilePath;
    }

    public static String serializeObject(Object obj) throws JsonProcessingException {
        return CustomJsonParser.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }

    public static <S> S deserializeObject(String jsonString, Class<S> clazz) throws JsonProcessingException {
        return CustomJsonParser.mapper.readValue(jsonString, clazz);
    }

    public static Map<String, Pair<String, String>> deserializeStringToStringPairMap(String jsonString)
            throws JsonProcessingException {
        return CustomJsonParser.mapper.readValue(jsonString, new TypeReference<>() {});
    }

    public T readJsonFile() {
        try {
            return CustomJsonParser.mapper.readValue(new File(this.jsonFilePath), this.objectClass);
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            Logger.err(CustomJsonParser.class, e);
            return null;
        }
    }

    public static <S> S readJsonFile(String jsonFilePath, Class<S> objectClass) {
        try {
            return CustomJsonParser.mapper.readValue(new File(jsonFilePath), objectClass);
        } catch (IOException e) {
            Logger.err(CustomJsonParser.class, e);
            return null;
        }
    }

    public void writeJsonFile(T obj) throws IOException {
        CustomJsonParser.mapper.writerWithDefaultPrettyPrinter().writeValue(new File(this.jsonFilePath), obj);
    }

    public static <S> void writeJsonFile(S obj, String jsonFilePath) throws IOException {
        CustomJsonParser.mapper.writerWithDefaultPrettyPrinter().writeValue(new File(jsonFilePath), obj);
    }
}
