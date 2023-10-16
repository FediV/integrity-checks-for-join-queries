package communication;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;
import utility.Logger;

import java.util.*;
import java.util.stream.Stream;

/**
 * The DataConfigFile class.
 * Payload for the POST request to /data/send-config.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class DataConfigFile {
    private long numberOfTuples;
    private List<Float> joinProportion;
    private String tableName;
    private Map<String,String> tableSchema;
    private String dbName;
    private String sourceFilePath;
    private boolean generateNewData;
    private float randomProbability;

    public long getNumberOfTuples() {
        return numberOfTuples;
    }

    public void setNumberOfTuples(long numberOfTuples) {
        this.numberOfTuples = numberOfTuples;
    }

    public List<Float> getJoinProportion() {
        return joinProportion;
    }

    public void setJoinProportion(List<Float> joinProportion) {
        this.joinProportion = joinProportion;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Map<String, String> getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(Map<String, String> tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getSourceFilePath() {
        return sourceFilePath;
    }

    public void setSourceFilePath(String sourceFilePath) {
        this.sourceFilePath = sourceFilePath;
    }

    public void setGenerateNewData(boolean generateNewData) {
        this.generateNewData = generateNewData;
    }

    public boolean getGenerateNewData() {
        return generateNewData;
    }

    public void setRandomProbability(float randomProbability) {
        this.randomProbability = randomProbability;
    }

    public float getRandomProbability() {
        return randomProbability;
    }

    public DataConfigFile() {}

    public DataConfigFile(long numberOfTuples, List<Float> joinProportion, Map<String,String> tableSchema,
                          String tableName, String dbName, String sourceFilePath, boolean generateNewData,
                          float randomProbability) {
        this.numberOfTuples = numberOfTuples;
        if (joinProportion != null && !joinProportion.isEmpty()) {
            float proportionSum = joinProportion.stream().reduce(0.0f, Float::sum);
            if (proportionSum < 1.0f) {
                joinProportion.add(1.0f - proportionSum);
            } else if (proportionSum > 1.0f) {
                Logger.warn(this, "Join proportion sum is > 1, using default 1%");
                joinProportion = Stream.generate(() -> 0.01f).limit(100).toList();
            }
        }
        this.joinProportion = joinProportion;
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.dbName = dbName;
        this.sourceFilePath = sourceFilePath;
        this.generateNewData = generateNewData;
        this.randomProbability = randomProbability;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataConfigFile that = (DataConfigFile) o;
        return numberOfTuples == that.numberOfTuples &&
                generateNewData == that.generateNewData
                && Float.compare(that.randomProbability, randomProbability) == 0
                && Objects.equals(joinProportion, that.joinProportion)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(tableSchema, that.tableSchema) &&
                Objects.equals(dbName, that.dbName) && Objects.equals(sourceFilePath, that.sourceFilePath);
    }

    @Override
    public int hashCode() {
        if (this.joinProportion != null && this.sourceFilePath != null) {
            return Objects.hash(this.numberOfTuples, this.joinProportion, this.tableName, this.tableSchema, this.dbName,
                    this.sourceFilePath);
        }
        if (this.joinProportion != null) {
            return Objects.hash(this.numberOfTuples, this.joinProportion, this.tableName, this.tableSchema, this.dbName);
        }
        if (this.sourceFilePath != null) {
            return Objects.hash(this.numberOfTuples, this.tableName, this.tableSchema, this.dbName, this.sourceFilePath);
        }
        return Objects.hash(this.numberOfTuples, this.tableName, this.tableSchema, this.dbName);
    }
}