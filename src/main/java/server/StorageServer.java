package server;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import simulation.SimulationConfigFile;
import utility.CustomJsonParser;
import utility.LogLevel;
import utility.Logger;
import utility.Network;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The StorageServer class.
 * Server functionality for the storage server.
 * Default address:
 *  127.0.0.1:1337 (L)
 *  127.0.0.1:1338 (R)
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class StorageServer {
    private static final String LOCAL_HOST = "localhost";
    private static final String SIMULATION_CONFIG_FILE_PATH = "./config/simulation.config.json";
    private String ipAddr;
    private int port;
    private HttpServer server;

    // SERVER VARIABLES FOR ALL SERVICES
    private static String name;
    private static String dbUrl;
    private static String joinAttribute;
    private static String tableName;

    private static final ClientConfig clientConfig = new ClientConfig();
    private static final Client client = ClientBuilder.newClient(clientConfig);
    private static String cspAddr;
    private static int cspPort;
    private static String clientAddr;
    private static int clientPort;
    private static final String JOIN_CSP_PATH = "join";
    private static final String DISTRIBUTED_JOIN_CSP_PATH = "distributed-join";

    private static final String NMAX_SALTS_CLIENT_PATH = "config/nmax-s";

    private StorageServer(String serverName, String ipAddr, int port, String dbPath) {
        StorageServer.setName(serverName);
        this.setIpAddr(ipAddr);
        this.setPort(port);
        StorageServer.setDbUrl("jdbc:derby:" + dbPath);

        SimulationConfigFile simulationConfigFile = CustomJsonParser.readJsonFile(SIMULATION_CONFIG_FILE_PATH, SimulationConfigFile.class);
        assert simulationConfigFile != null;
        Map<String, String> cspParams = simulationConfigFile.getComputationalServer();
        Map<String, String> clientParams = simulationConfigFile.getClient();
        assert cspParams != null && clientParams != null &&
                cspParams.containsKey("ipAddr") && cspParams.containsKey("port") &&
                clientParams.containsKey("ipAddr") && clientParams.containsKey("port");
        StorageServer.cspAddr = cspParams.get("ipAddr");
        StorageServer.cspPort = Integer.parseInt(cspParams.get("port"));
        StorageServer.clientAddr = clientParams.get("ipAddr");
        StorageServer.clientPort = Integer.parseInt(clientParams.get("port"));
    }

    // Handle REST requests
    public static void main(String[] args) {
        Logger.info(StorageServer.class, Arrays.toString(args), LogLevel.REQUIRED);
        StorageServer storageServer = null;
        switch (args.length) {
            case 3 -> storageServer = new StorageServer(args[0], "127.0.0.1", Integer.parseInt(args[1]), args[2]);
            case 4 -> storageServer = new StorageServer(args[0], args[3], Integer.parseInt(args[1]), args[2]);
            default -> {
                Logger.err(StorageServer.class, "Not enough arguments to start the storage server:\n" +
                        "Please use: <serverName> <port> <dbName> {<serverAddress>}");
                System.exit(1);
            }
        }

        try {
            storageServer.startServer();
            storageServer.stopServer(5);
        } catch (IOException e) {
            try {
                storageServer.setIpAddr(StorageServer.LOCAL_HOST);
                storageServer.startServer();
                storageServer.stopServer(10);
            } catch (IOException ex) {
                Logger.err(StorageServer.class,"Unable to create an HTTP server on " + storageServer.getPort());
                ex.printStackTrace();
            }
        }
    }

    private void startServer() throws IOException {
        ResourceConfig config = new ResourceConfig().packages("server.services");
        this.server = GrizzlyHttpServerFactory.createHttpServer(URI.create(Network.createURL(this.getIpAddr(), this.getPort())), config);
        this.server.start();
        Logger.info(this, "Server " + StorageServer.getName() + " running!", LogLevel.REQUIRED);
        Logger.info(this, "Server running at: " + this, LogLevel.REQUIRED);
    }

    private void stopServer(int delay) throws IOException {
        Logger.info(this,"Press return to stop...", LogLevel.REQUIRED);
        System.in.read();
        Logger.info(this,"Stopping server...", LogLevel.REQUIRED);
        server.shutdown(delay, TimeUnit.MILLISECONDS);
        Logger.info(this,"Server stopped", LogLevel.REQUIRED);
        System.exit(0);
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public void setPort(int port) {
        if (port >= 0 && port <= 65535) {
            this.port = port;
        } else {
            this.port = 1337;
            Logger.err(this, "Invalid port number: " + port + " | port set to default (1337)");
        }
    }

    public String getIpAddr() {
        return this.ipAddr;
    }

    public int getPort() {
        return this.port;
    }

    public static void setDbUrl(String dbUrl) {
        StorageServer.dbUrl = dbUrl;
    }

    public static String getDbUrl() {
        return StorageServer.dbUrl;
    }

    public static void setJoinAttribute(String joinAttribute) {
        StorageServer.joinAttribute = joinAttribute;
    }

    public static String getJoinAttribute() {
        return StorageServer.joinAttribute;
    }

    public static void setTableName(String tableName) {
        StorageServer.tableName = tableName;
    }

    public static String getTableName() {
        return StorageServer.tableName;
    }

    public static String getName() {
        return StorageServer.name;
    }

    public static void setName(String name) {
        StorageServer.name = name;
    }

    public static Client getClient() {
        return StorageServer.client;
    }

    public static String getCspAddr() {
        return StorageServer.cspAddr;
    }

    public static int getCspPort() {
        return StorageServer.cspPort;
    }

    public static String getJoinCspPath() {
        return StorageServer.JOIN_CSP_PATH;
    }

    public static String getClientAddr() {
        return StorageServer.clientAddr;
    }

    public static int getClientPort() {
        return StorageServer.clientPort;
    }

    public static String getDistributedJoinCspPath() {
        return StorageServer.DISTRIBUTED_JOIN_CSP_PATH;
    }

    public static String getNmaxSaltsClientPath() {
        return StorageServer.NMAX_SALTS_CLIENT_PATH;
    }

    public static String addServerName(String s) {
            return s + "-" + StorageServer.getName();
    }

    @Override
    public String toString() {
        return Network.createURL(this.getIpAddr(), this.getPort());
    }
}
