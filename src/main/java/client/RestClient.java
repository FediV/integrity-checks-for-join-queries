package client;

import communication.*;
import jakarta.ws.rs.client.*;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientConfig;
import simulation.SimulationConfigFile;
import utility.*;

import javax.crypto.SecretKey;
import java.sql.Connection;
import java.util.*;

import static utility.Network.createURL;

/**
 * The RestClient class.
 * The main client class.
 * It requires the config files:
 *  simulation.config.json
 *  db.config.json
 *  client.config.json
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class RestClient {
    private static final String CLIENT_CONFIG_FILE_PATH = "./config/client.config.json";
    private static ClientConfigFile clientConfigFile;
    private static final String DB_CONFIG_FILE_PATH = "./config/db.config.json";
    private static final String SIMULATION_CONFIG_FILE_PATH = "./config/simulation.config.json";

    private static DbConfigFile dbConfigFile;
    private static DataConfigFile dataConfigFileL;
    private static DataConfigFile dataConfigFileR;

    public static HttpServerThread restServer;
    private static final String DATA_SEND_CONFIG_PATH = "data/send-config";
    private static final String DATA_SEND_PARAMS_PATH = "data/send-params";
    private static final String DATA_GENERATE_PATH = "data/generate";
    private static final String QUERY_MSG_PATH = "query/process";
    private static final String SEMI_JOIN_PATH = "query/semi-join";
    private static final String NUMBER_OF_SALTS_PATH = "query/number-of-salts";

    private static final ClientConfig clientConfig = new ClientConfig();
    private static final Client client = ClientBuilder.newClient(clientConfig);

    private static String clientTableName;
    private static String storageServerAddrL;
    private static String storageServerAddrR;
    private static int storageServerPortL;
    private static int storageServerPortR;

    private static String dbUrl;
    public static Connection connect;

    private static Long joinRngSeed;

    private static String joinAttribute;

    private static String joinAttributeFilePath;
    private static SecretKey encryptionKey;
    private static byte[] encryptionVector;

    private static SecretKey hashKey;

    public static void main(String[] args) {
        // CREATE AND START SERVER THREAD
        Logger.info(RestClient.class, Arrays.toString(args), LogLevel.REQUIRED);
        switch (args.length) {
            case 3 -> {
                RestClient.restServer = new HttpServerThread(args[0], "127.0.0.1", Integer.parseInt(args[1]));
                RestClient.dbUrl = "jdbc:derby:" + args[2];
            }
            case 4 -> {
                RestClient.restServer = new HttpServerThread(args[0], args[3], Integer.parseInt(args[1]));
                RestClient.dbUrl = "jdbc:derby:" + args[2];
            }
            default -> {
                Logger.err(RestClient.class, "Not enough arguments to start the storage server:\n" +
                        "Please use: <serverName> <port> <dbName> {<serverAddress>}");
                System.exit(1);
            }
        }
        RestClient.restServer.start();
    }

    public static void execute() {
        SimulationConfigFile simulationConfigFile = CustomJsonParser.readJsonFile(SIMULATION_CONFIG_FILE_PATH, SimulationConfigFile.class);
        assert simulationConfigFile != null;
        Map<String, String> clientParams = simulationConfigFile.getClient();
        Map<String, String> serverLParams = simulationConfigFile.getStorageServer1();
        Map<String, String> serverRParams = simulationConfigFile.getStorageServer2();
        assert clientParams != null && serverRParams != null && serverLParams != null;
        RestClient.dbUrl = "jdbc:derby:" + clientParams.get("dbPath");
        RestClient.clientTableName = clientParams.get("tableName");
        RestClient.storageServerAddrL = serverLParams.get("ipAddr");
        RestClient.storageServerAddrR = serverRParams.get("ipAddr");
        RestClient.storageServerPortL = Integer.parseInt(serverLParams.get("port"));
        RestClient.storageServerPortR = Integer.parseInt(serverRParams.get("port"));

        // START DB CONNECTION
        RestClient.connect = ModelUtils.startDBConnection(RestClient.dbUrl);
        ModelUtils.clearTables(true, new ArrayList<>() {{
            add(RestClient.clientTableName);
        }}, RestClient.connect);

        // GET AND PARSE THE CONFIG FILES
        RestClient.clientConfigFile =
                CustomJsonParser.readJsonFile(CLIENT_CONFIG_FILE_PATH, ClientConfigFile.class);

        RestClient.dbConfigFile =
                CustomJsonParser.readJsonFile(DB_CONFIG_FILE_PATH, DbConfigFile.class);

        // SEND DATA CONFIG FILE TO THE STORAGE SERVERS TO GENERATE DATA
        assert RestClient.dbConfigFile != null;
        RestClient.joinAttribute = RestClient.dbConfigFile.getJoinAttribute();
        RestClient.joinRngSeed = RestClient.dbConfigFile.getSeed();
        RestClient.joinAttributeFilePath = RestClient.dbConfigFile.getJoinAttributeFilePath();
        RestClient.dataConfigFileL = RestClient.dbConfigFile.getDbs().get("L");
        RestClient.dataConfigFileR = RestClient.dbConfigFile.getDbs().get("R");

        StringBuilder createQuery = new StringBuilder("CREATE TABLE " +
                RestClient.clientTableName +
                " (id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, " +
                RestClient.dataConfigFileL.getTableName() + "_ID VARCHAR(255), ");
        for (Map.Entry<String, String> e : RestClient.dataConfigFileL.getTableSchema().entrySet()) {
            createQuery
                    .append(RestClient.dataConfigFileL.getTableName())
                    .append("_")
                    .append(e.getKey())
                    .append(" VARCHAR(255), ");
        }
        createQuery.append(RestClient.dataConfigFileR.getTableName()).append("_ID VARCHAR(255), ");
        for (Map.Entry<String, String> e : RestClient.dataConfigFileR.getTableSchema().entrySet()) {
            createQuery
                    .append(RestClient.dataConfigFileR.getTableName())
                    .append("_")
                    .append(e.getKey())
                    .append(" VARCHAR(255), ");
        }
        createQuery.replace(createQuery.length() - 2, createQuery.length(), ")");
        Logger.info(RestClient.class, String.valueOf(createQuery), LogLevel.COMPLETE);
        ModelUtils.modifyDB(String.valueOf(createQuery), RestClient.connect);

        // SEND THE QUERY MESSAGES TO THE STORAGE SERVERS WITH INSTRUCTIONS FOR MARKERS AND TWINS
        assert RestClient.clientConfigFile != null;
        RestClient.encryptionKey = Encryption.createAESKey();
        RestClient.encryptionVector = Encryption.createInitializationVector();
        RestClient.hashKey = Encryption.createSHA3Key();
        TwinCondition twinCondition = clientConfigFile.getTwinCondition();
        List<String> workers = clientConfigFile.getWorkers();
        boolean isSemiJoin = clientConfigFile.getIsSemiJoin();
        boolean useOccurences = clientConfigFile.getUseOccurrences();
        int minNumberOfMarkers = clientConfigFile.getMinNumberOfMarkers();
        int maxNumberOfMarkers = clientConfigFile.getMaxNumberOfMarkers();
        int numberOfMarkers = clientConfigFile.getNumberOfMarkers();
        int replicationFactor = clientConfigFile.getReplicationFactor();

        String encryptionString = Base64.getEncoder().encodeToString(
                Objects.requireNonNull(RestClient.encryptionKey).getEncoded()
        );
        String hashString = Base64.getEncoder().encodeToString(
                Objects.requireNonNull(RestClient.hashKey).getEncoded()
        );

        QueryMessage queryMessageL = RestClient.clientConfigFile.getQueryMessages().get("L");
        queryMessageL.setAESkey(encryptionString);
        queryMessageL.setAESinitVector(RestClient.getEncryptionVector());
        queryMessageL.setSHAkey(hashString);
        queryMessageL.setTwinCondition(twinCondition);
        queryMessageL.setWorkers(workers);
        queryMessageL.setIsSemiJoin(isSemiJoin);
        queryMessageL.setUseOccurrences(useOccurences);
        queryMessageL.setMinNumberOfMarkers(minNumberOfMarkers);
        queryMessageL.setMaxNumberOfMarkers(maxNumberOfMarkers);
        queryMessageL.setNumberOfMarkers(numberOfMarkers);
        queryMessageL.setReplicationFactor(replicationFactor);

        QueryMessage queryMessageR = RestClient.clientConfigFile.getQueryMessages().get("R");
        queryMessageR.setAESkey(encryptionString);
        queryMessageR.setAESinitVector(RestClient.getEncryptionVector());
        queryMessageR.setSHAkey(hashString);
        queryMessageR.setTwinCondition(twinCondition);
        queryMessageR.setWorkers(workers);
        queryMessageR.setIsSemiJoin(isSemiJoin);
        queryMessageR.setUseOccurrences(useOccurences);
        queryMessageR.setMinNumberOfMarkers(minNumberOfMarkers);
        queryMessageR.setMaxNumberOfMarkers(maxNumberOfMarkers);
        queryMessageR.setNumberOfMarkers(numberOfMarkers);
        queryMessageR.setReplicationFactor(replicationFactor);

        Response response;
        response = RestClient.generateDataStorageServer(RestClient.storageServerAddrL, RestClient.storageServerPortL,
                    RestClient.dataConfigFileL, dataConfigFileL.getGenerateNewData());
        Logger.info(RestClient.class, "Server L response status: " + response.getStatus(), LogLevel.COMPLETE);
        response = RestClient.generateDataStorageServer(RestClient.storageServerAddrR, RestClient.storageServerPortR,
                    RestClient.dataConfigFileR, dataConfigFileR.getGenerateNewData());
        Logger.info(RestClient.class, "Server R response status: " + response.getStatus(), LogLevel.COMPLETE);
        
        Thread queryThreadL = new Thread(() -> {
            Response responseL = RestClient.sendQueryStorageServer(RestClient.storageServerAddrL, RestClient.storageServerPortL, queryMessageL);
            Logger.info(RestClient.class, "Server L response status: " + responseL.getStatus(), LogLevel.COMPLETE);
        });

        Thread queryThreadR = new Thread(() -> {
            Response responseR = RestClient.sendQueryStorageServer(RestClient.storageServerAddrR, RestClient.storageServerPortR, queryMessageR);
            Logger.info(RestClient.class, "Server R response status: " + responseR.getStatus(), LogLevel.COMPLETE);
        });

        queryThreadL.start();
        queryThreadR.start();
        try {
            queryThreadL.join();
            queryThreadR.join();
        } catch (InterruptedException e) {
            Logger.err(RestClient.class, e);
        }
    }

    private static Response generateDataStorageServer(String serverAddr, int serverPort, DataConfigFile dataConfigFile, boolean generate) {
        Response response;
        response = Network.post(client,
                createURL(serverAddr, serverPort, DATA_SEND_CONFIG_PATH),
                dataConfigFile);
        Logger.info(RestClient.class, "Send config file response status: " + response.getStatus(), LogLevel.COMPLETE);

        response = Network.setParams(client,
                createURL(serverAddr, serverPort, DATA_SEND_PARAMS_PATH),
                RestClient.joinRngSeed,
                RestClient.joinAttribute,
                RestClient.joinAttributeFilePath);
        Logger.info(RestClient.class, "Send params response status: " + response.getStatus(), LogLevel.COMPLETE);

        if (generate) {
            response = Network.get(client, createURL(serverAddr, serverPort, DATA_GENERATE_PATH));
            Logger.info(RestClient.class, "Generate data response status: " + response.getStatus(), LogLevel.COMPLETE);
        }
        return response;
    }

    private static Response sendQueryStorageServer(String serverAddr, int serverPort, QueryMessage queryMessage) {
        Response response;
        response = Network.post(client,
                createURL(serverAddr, serverPort, QUERY_MSG_PATH),
                queryMessage);
        Logger.info(RestClient.class, "Send query message response status: " + response.getStatus(), LogLevel.COMPLETE);

        return response;
    }

    public static SecretKey getEncryptionKey() {
        return RestClient.encryptionKey;
    }

    public static byte[] getEncryptionVector() {
        return RestClient.encryptionVector;
    }

    public static ClientConfigFile getClientConfigFile() {
        return RestClient.clientConfigFile;
    }

    public static SecretKey getHashKey() {
        return RestClient.hashKey;
    }

    public static Client getClient() {
        return RestClient.client;
    }

    public static String getSemiJoinPath() {
        return RestClient.SEMI_JOIN_PATH;
    }

    public static String getClientTableName() {
        return RestClient.clientTableName;
    }

    public static String getDbUrl() {
        return RestClient.dbUrl;
    }

    public static String getStorageServerAddrL() {
        return RestClient.storageServerAddrL;
    }

    public static int getStorageServerPortL() {
        return RestClient.storageServerPortL;
    }

    public static String getStorageServerAddrR() {
        return RestClient.storageServerAddrR;
    }

    public static int getStorageServerPortR() {
        return RestClient.storageServerPortR;
    }

    public static String getNumberOfSaltsPath() {
        return RestClient.NUMBER_OF_SALTS_PATH;
    }
}
