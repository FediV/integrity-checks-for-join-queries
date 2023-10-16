package model;

import communication.DataConfigFile;
import org.apache.commons.lang3.RandomStringUtils;
import server.StorageServer;
import utility.LogLevel;
import utility.Logger;
import utility.ModelUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * The DataGenerator class.
 * Manage the data generation process.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class DataGenerator {
    private static final Random RNG = new Random();
    private static final int MIN_AGE = 18;
    private static final int MAX_AGE = 100;
    private static Random joinRng;
    private final DataConfigFile dataConfigFile;
    private final Connection connect;
    private final Map<String,List<String>> entities;
    private static Long seed;
    private static String joinAttributeFilePath;

    public DataGenerator(DataConfigFile dataConfigFile) {
        this.dataConfigFile = dataConfigFile;
        this.entities = new HashMap<>();
        StorageServer.setTableName(dataConfigFile.getTableName());
        this.connect = ModelUtils.startDBConnection(StorageServer.getDbUrl());
        if (dataConfigFile.getGenerateNewData()) {
            ModelUtils.clearTables(true, new ArrayList<>() {{ add("TABLER"); add("TABLEL"); }}, this.connect);
            this.createTable();
        }
    }

    private void createListFromFile(String filePath, String name) {
        this.entities.computeIfAbsent(name, key -> new ArrayList<>());
        try {
            Scanner scanner = new Scanner(new File(filePath)).useDelimiter(Pattern.compile("[\\r\\n]+"));
            while(scanner.hasNext()) {
                this.entities.get(name).add(scanner.next());
            }
        } catch(FileNotFoundException e) {
            Logger.err(this, e);
        }
    }

    private void createTable() {
        StringBuilder createQuery = new StringBuilder("CREATE TABLE " +
                this.dataConfigFile.getTableName() +
                " (id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, ");
        for (Map.Entry<String, String> e: this.dataConfigFile.getTableSchema().entrySet()) {
            createQuery.append(e.getKey()).append(" ").append(e.getValue()).append(", ");
        }
        createQuery.replace(createQuery.length() - 2, createQuery.length(), ")");
        Logger.info(this, String.valueOf(createQuery), LogLevel.COMPLETE);
        ModelUtils.modifyDB(createQuery.toString(), this.connect);
    }

    // Data generation
    public void generate(List<String> dataGenerationFiles) {
        if (this.dataConfigFile == null) {
            Logger.err(this, "Cannot generate data, config file is null!");
            return;
        }
        if (dataGenerationFiles == null) {
            dataGenerationFiles = new ArrayList<>();
        }
        dataGenerationFiles.add(DataGenerator.getJoinAttributeFilePath());
        dataGenerationFiles.forEach(f -> {
            if (f.contains("/")) {
                this.createListFromFile(f, f.substring(f.lastIndexOf('/') + 1, f.lastIndexOf('.')));
            } else {
                this.createListFromFile(f, f.substring(0, f.lastIndexOf('.')));
            }
        });
        long numberOfTuples = this.dataConfigFile.getNumberOfTuples();
        List<Float> joinProportion = this.dataConfigFile.getJoinProportion();

        Map<String, String> attributeList = this.dataConfigFile.getTableSchema();
        attributeList.remove(StorageServer.getJoinAttribute());
        try {
            this.populateRelations(numberOfTuples, joinProportion, attributeList.keySet().iterator().next());
        } catch (SQLException e) {
            Logger.err(this, e);
        } finally {
            ModelUtils.closeDBConnection(this.connect);
        }
    }

    private void populateRelations(long numberOfTuples, List<Float> joinProportion, String attribute) throws SQLException {
        boolean intAttribute = Objects.equals(this.dataConfigFile.getTableSchema().get(attribute), "INTEGER");
        float randomProbability = this.dataConfigFile.getRandomProbability();
        assert randomProbability >= 0.0 && randomProbability <= 1.0;
        int joinAttrSize = this.entities.get(StorageServer.getJoinAttribute()).size();
        int joinAttributeIndex = DataGenerator.joinRng.nextInt(0, joinAttrSize);
        Logger.info(this, "***************** Join attr. index: " + joinAttributeIndex + " *****************",
                LogLevel.REQUIRED);

        if (joinProportion == null || joinProportion.isEmpty()) {
            Logger.info(this, "Join proportions not specified: using default all different policy...", LogLevel.REQUIRED);
            this.insertBatch(attribute, joinAttributeIndex, joinAttrSize, randomProbability, intAttribute, numberOfTuples);
        } else {
            int numberOfBuckets = joinProportion.size();
            int[] proportions = new int[numberOfBuckets];
            BigDecimal decimals = BigDecimal.ZERO;
            for (int i = 0; i < numberOfBuckets; i++) {
                BigDecimal n = BigDecimal.valueOf(numberOfTuples).multiply(BigDecimal.valueOf(joinProportion.get(i)));
                proportions[i] = n.intValue();
                decimals = decimals.add(n.subtract(new BigDecimal(proportions[i])));
                if (decimals.compareTo(BigDecimal.ONE) > 0) {
                    proportions[i]++;
                    decimals = decimals.subtract(BigDecimal.ONE);
                }
            }
            int proportionSum = (int) Arrays.stream(proportions).asLongStream().sum();
            if (proportionSum < numberOfTuples) {
                proportions[numberOfBuckets - 1] += numberOfTuples - proportionSum;
            }
            Logger.info(this, "Number of buckets: " + proportions.length + " | " +
                     "Tuples per bucket " + Arrays.toString(proportions), LogLevel.COMPLETE);
            int randomProbabilityCounter = 0;
            for (int i = 0; i < numberOfBuckets; i++) {
                String value = this.entities.get(StorageServer.getJoinAttribute()).get((joinAttributeIndex + i) % joinAttrSize);
                if (joinAttributeIndex + i >= joinAttrSize) {
                    value = value + (randomProbabilityCounter++ % Integer.MAX_VALUE);
                }
                this.insertBatchAllEquals(intAttribute, proportions[i], attribute, value, randomProbability);
            }
        }
        this.connect.commit();
    }

    private void insertBatch(String attributeName, int joinAttributeIndex, int joinAttrSize, float randomProbability,
                             boolean intAttribute, long numberOfTuples) throws SQLException {
        PreparedStatement insertStatement =
                this.connect.prepareStatement("INSERT INTO " +
                        this.dataConfigFile.getTableName() +
                        "(" + StorageServer.getJoinAttribute() + "," + attributeName + ") VALUES (?,?)");
        this.connect.setAutoCommit(false);
        int randomProbabilityCounter = 0;
        if (randomProbability > 0.0) {
            if (intAttribute) {
                for (int i = 0; i < numberOfTuples; i++) {
                    String joinAttrValue = this.entities
                            .get(StorageServer.getJoinAttribute())
                            .get((joinAttributeIndex + i) % joinAttrSize);
                    if (RNG.nextDouble() > randomProbability) {
                        joinAttrValue = this.entities
                                .get(StorageServer.getJoinAttribute())
                                .get(RNG.nextInt(0, this.entities.get(StorageServer.getJoinAttribute()).size()));
                    }
                    insertStatement.setString(1, joinAttrValue);
                    insertStatement.setInt(2, RNG.nextInt(MIN_AGE, MAX_AGE));
                    insertStatement.addBatch();
                    if (i % ModelUtils.BATCH_SIZE == 0 || i == numberOfTuples - 1) {
                        insertStatement.executeBatch();
                        this.connect.commit();
                    }
                }
            } else {
                for (int i = 0; i < numberOfTuples; i++) {
                    String joinAttrValue = this.entities
                            .get(StorageServer.getJoinAttribute())
                            .get((joinAttributeIndex + i) % joinAttrSize);
                    if (RNG.nextDouble() > randomProbability) {
                        joinAttrValue = this.entities
                                .get(StorageServer.getJoinAttribute())
                                .get(RNG.nextInt(0, this.entities.get(StorageServer.getJoinAttribute()).size()));
                    }
                    insertStatement.setString(1, joinAttrValue);
                    this.setVarcharAttrValue(attributeName, numberOfTuples, insertStatement, i);
                }
            }
        } else {
            if (intAttribute) {
                for (int i = 0; i < numberOfTuples; i++) {
                    String joinAttrValue = this.entities
                            .get(StorageServer.getJoinAttribute())
                            .get((joinAttributeIndex + i) % joinAttrSize);
                    if (joinAttributeIndex + i >= joinAttrSize) {
                        joinAttrValue = joinAttrValue + (randomProbabilityCounter++ % Integer.MAX_VALUE);
                    }
                    insertStatement.setString(1, joinAttrValue);
                    insertStatement.setInt(2, RNG.nextInt(MIN_AGE, MAX_AGE));
                    insertStatement.addBatch();
                    if (i % ModelUtils.BATCH_SIZE == 0 || i == numberOfTuples - 1) {
                        insertStatement.executeBatch();
                        this.connect.commit();
                    }
                }
            } else {
                for (int i = 0; i < numberOfTuples; i++) {
                    String joinAttrValue = this.entities
                            .get(StorageServer.getJoinAttribute())
                            .get((joinAttributeIndex + i) % joinAttrSize);
                    if (joinAttributeIndex + i >= joinAttrSize) {
                        joinAttrValue = joinAttrValue + (randomProbabilityCounter++ % Integer.MAX_VALUE);
                    }
                    insertStatement.setString(1, joinAttrValue);
                    this.setVarcharAttrValue(attributeName, numberOfTuples, insertStatement, i);
                }
            }
        }
        this.connect.setAutoCommit(true);
    }

    private void setVarcharAttrValue(String attributeName, long numberOfTuples, PreparedStatement insertStatement,
                                     int index) throws SQLException {
        if (this.entities.get(attributeName) == null) {
            String attrSchema = this.dataConfigFile.getTableSchema().get(attributeName);
            assert attrSchema.toUpperCase().startsWith("VARCHAR");
            int maxLength = Integer.parseInt(
                    attrSchema.substring(attrSchema.indexOf('(') + 1, attrSchema.lastIndexOf(')'))
            );
            insertStatement.setString(2, RandomStringUtils.random(Math.min(15, maxLength), true, true));
        } else {
            insertStatement.setString(2,
                    this.entities
                            .get(attributeName)
                            .get(RNG.nextInt(0, this.entities.get(attributeName).size())));
        }
        insertStatement.addBatch();
        if (index % ModelUtils.BATCH_SIZE == 0 || index == numberOfTuples - 1) {
            insertStatement.executeBatch();
            this.connect.commit();
        }
    }

    private void insertBatchAllEquals(boolean intAttribute, int numberOfTimes, String attributeName, String attributeValue,
                        float randomProbability) throws SQLException {
        PreparedStatement insertStatement =
                this.connect.prepareStatement("INSERT INTO " +
                        this.dataConfigFile.getTableName() +
                        "(" + StorageServer.getJoinAttribute() + "," + attributeName + ") VALUES (?,?)");
        if (randomProbability > 0.0) {
            if (RNG.nextDouble() > randomProbability) {
                attributeValue = this.entities
                        .get(StorageServer.getJoinAttribute())
                        .get(RNG.nextInt(0, this.entities.get(StorageServer.getJoinAttribute()).size()));
            }
        }

        this.connect.setAutoCommit(false);
        if (intAttribute) {
            for (int j = 0; j < numberOfTimes; j++) {
                insertStatement.setString(1, attributeValue);
                insertStatement.setInt(2, RNG.nextInt(MIN_AGE, MAX_AGE));
                insertStatement.addBatch();
                if (j % ModelUtils.BATCH_SIZE == 0 || j == numberOfTimes - 1) {
                    insertStatement.executeBatch();
                    this.connect.commit();
                }
            }
        } else {
            for (int j = 0; j < numberOfTimes; j++) {
                insertStatement.setString(1, attributeValue);
                this.setVarcharAttrValue(attributeName, numberOfTimes, insertStatement, j);
            }
        }
        this.connect.setAutoCommit(true);
    }

    public static void setSeed(Long seed) {
        if (DataGenerator.joinRng == null) {
            DataGenerator.seed = seed;
            DataGenerator.joinRng = DataGenerator.seed == -1 ? new Random() : new Random(DataGenerator.seed);
        }
    }

    public static Long getSeed() {
        return DataGenerator.seed;
    }

    public static String getJoinAttributeFilePath() {
        return joinAttributeFilePath;
    }

    public static void setJoinAttributeFilePath(String joinAttributeFilePath) {
        DataGenerator.joinAttributeFilePath = joinAttributeFilePath;
    }
}
