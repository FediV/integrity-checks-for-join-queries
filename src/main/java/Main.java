import client.RestClient;
import client.services.ResultService;
import communication.ClientConfigFile;
import communication.DbConfigFile;
import model.QueryHandler;
import server.ComputationalServer;
import server.StorageServer;
import simulation.SimulationConfigFile;
import simulation.SimulationOutput;
import utility.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Main class.
 * Entry point of the simulation prototype.
 * It requires the jar files:
 *  StorageServer.jar
 *  ComputationalServer.jar
 * as well as the config files:
 *  simulation.config.json
 *  db.config.json
 *  client.config.json.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class Main {
    private static final String SIMULATION_CONFIG_FILE_PATH = "./config/simulation.config.json";
    private static final String DB_CONFIG_FILE_PATH = "./config/db.config.json";
    private static final String CLIENT_CONFIG_FILE_PATH = "./config/client.config.json";
    private static final String STORAGE_SERVER_JAR_FILE_PATH = "./jars/StorageServer.jar";
    private static final String COMPUTATIONAL_SERVER_JAR_FILE_PATH = "./jars/ComputationalServer.jar";
    private static final String STORAGE_SERVER_MAIN_CLASS = StorageServer.class.getCanonicalName();
    private static final String COMPUTATIONAL_SERVER_MAIN_CLASS = ComputationalServer.class.getCanonicalName();
    private static final int PROCESS_START_TIMEOUT = 3000;
    private static final String MAX_CSP_HEAP_SIZE = "-Xmx512M";
    private static final String MAX_STORAGE_HEAP_SIZE = "-Xmx256M";
    private static final String STACK_THREAD_SIZE = "-Xss2048K";

    public static void main(String[] args) {
        File regularTempFile = new File(QueryHandler.REGULAR_TEMP_FILE_PATH);
        File twinTempFile = new File(QueryHandler.TWIN_TEMP_FILE_PATH);
        File occurrencesTempFile = new File(QueryHandler.OCCURRENCES_TEMP_FILE_PATH);
        File clientTempFile = new File(ResultService.CLIENT_TEMP_FILE_PATH);
        assert !regularTempFile.exists() || regularTempFile.delete();
        assert !twinTempFile.exists() || twinTempFile.delete();
        assert !occurrencesTempFile.exists() || occurrencesTempFile.delete();
        assert !clientTempFile.exists() || clientTempFile.delete();

        SimulationConfigFile simulationConfigFile = CustomJsonParser.readJsonFile(SIMULATION_CONFIG_FILE_PATH, SimulationConfigFile.class);
        DbConfigFile dbConfigFile = CustomJsonParser.readJsonFile(DB_CONFIG_FILE_PATH, DbConfigFile.class);
        ClientConfigFile clientConfigFile = CustomJsonParser.readJsonFile(CLIENT_CONFIG_FILE_PATH, ClientConfigFile.class);
        assert simulationConfigFile != null && dbConfigFile != null && clientConfigFile != null;
        List<SimulationOutput> simulationOutputs = new ArrayList<>();

        switch (simulationConfigFile.getLoggerLevel()) {
            case 0 -> Logger.level = LogLevel.ERROR;
            case 1 -> Logger.level = LogLevel.WARNING;
            case 2 -> Logger.level = LogLevel.REQUIRED;
            default -> Logger.level = LogLevel.COMPLETE;
        }

        Map<String, String> storageServerL = simulationConfigFile.getStorageServer1();
        Map<String, String> storageServerR = simulationConfigFile.getStorageServer2();
        Map<String, String> computationalServer = simulationConfigFile.getComputationalServer();
        Map<String, String> client= simulationConfigFile.getClient();
        assert storageServerL != null && storageServerR != null && computationalServer != null && client != null;
        boolean overwriteStatisticsFile = simulationConfigFile.getOverwriteStatisticsFile();

        String dbClientPath = client.get("dbPath");
        String dbLPath = dbConfigFile.getDbs().get("L").getDbName();
        String dbRPath = dbConfigFile.getDbs().get("R").getDbName();

        Process processL = Main.startProcess(storageServerL, STORAGE_SERVER_JAR_FILE_PATH, STORAGE_SERVER_MAIN_CLASS,
                dbLPath);
        assert processL != null && processL.isAlive();

        Process processR = Main.startProcess(storageServerR, STORAGE_SERVER_JAR_FILE_PATH, STORAGE_SERVER_MAIN_CLASS,
                dbRPath);
        assert processR != null && processR.isAlive();

        Process processCSP = Main.startProcess(computationalServer, COMPUTATIONAL_SERVER_JAR_FILE_PATH, COMPUTATIONAL_SERVER_MAIN_CLASS,
                null);
        assert processCSP != null && processCSP.isAlive();

        try {
            Thread.sleep(PROCESS_START_TIMEOUT);
        } catch (InterruptedException e) {
            Logger.err(Main.class, e);
        }

        Thread clientThread = new Thread(() ->
                RestClient.main(new String[]{
                        client.get("name"), client.get("port"), dbClientPath, client.get("ipAddr")
                }));
        clientThread.start();
        try {
            for (int i = 0; i < simulationConfigFile.getNumberOfRuns(); i++) {
                SimulationOutput simulationOutput = new SimulationOutput();
                long startTime = System.nanoTime();
                Thread exec = new Thread(RestClient::execute);
                exec.start();
                exec.join();
                long endTime = System.nanoTime();
                simulationOutput.setElapsedTime((endTime - startTime) / 1000000);
                simulationOutput.setNumberOfMarkers(clientConfigFile.getNumberOfMarkers());
                simulationOutputs.add(simulationOutput);
            }
            clientThread.interrupt();
            processCSP.destroy();
            processL.destroy();
            processR.destroy();

            processCSP.waitFor();
            processL.waitFor();
            processR.waitFor();
        } catch (InterruptedException e) {
            Logger.err(Main.class, e);
        }

        boolean useOccurrences = clientConfigFile.getUseOccurrences();

        if (!useOccurrences) {
            BufferedReader twinTempReader, clientTempReader, regularTempReader;
            try {
                twinTempReader = new BufferedReader(new FileReader(QueryHandler.TWIN_TEMP_FILE_PATH));
                clientTempReader = new BufferedReader(new FileReader(ResultService.CLIENT_TEMP_FILE_PATH));
                regularTempReader = new BufferedReader(new FileReader(QueryHandler.REGULAR_TEMP_FILE_PATH));

                for (int i = 0; i < simulationConfigFile.getNumberOfRuns(); i++) {
                    SimulationOutput simulationOutput = simulationOutputs.get(i);
                    simulationOutput.setRunId(i + 1);
                    Logger.info(Main.class, "------------------- RUN " + (i + 1) + " -------------------", LogLevel.REQUIRED);
                    Logger.info(Main.class, "Elapsed time: " + simulationOutput.getElapsedTime() + " ms", LogLevel.REQUIRED);

                    String[] twins1 = twinTempReader.readLine().split(":");
                    String[] twins2 = twinTempReader.readLine().split(":");
                    assert twins1.length == 3 && twins2.length == 3;
                    if (twins1[0].equals(storageServerL.get("name"))) {
                        assert twins2[0].equals(storageServerR.get("name"));
                        simulationOutput.setNumberOfTwinsL(Integer.parseInt(twins1[1]));
                        simulationOutput.setNumberOfTwinsR(Integer.parseInt(twins2[1]));
                        simulationOutput.setTableLsize(twins1[2]);
                        simulationOutput.setTableRsize(twins2[2]);
                    } else {
                        assert twins1[0].equals(storageServerR.get("name"));
                        simulationOutput.setNumberOfTwinsL(Integer.parseInt(twins2[1]));
                        simulationOutput.setNumberOfTwinsR(Integer.parseInt(twins1[1]));
                        simulationOutput.setTableLsize(twins2[2]);
                        simulationOutput.setTableRsize(twins1[2]);
                    }
                    Logger.info(Main.class, "Storage server " +  storageServerL.get("name") + " table size: " + simulationOutput.getTableLsize(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Storage server " +  storageServerR.get("name") + " table size: " + simulationOutput.getTableRsize(), LogLevel.REQUIRED);

                    String[] clientData = clientTempReader.readLine().split(":");
                    assert clientData.length == 7;
                    simulationOutput.setResultTableSize(clientData[0]);
                    simulationOutput.setResultTableTuples(Integer.parseInt(clientData[1]));
                    if (!clientData[2].equals("null")) {
                        simulationOutput.setTotalControlTuples(Integer.parseInt(clientData[2]));
                    }
                    simulationOutput.setControlTuplesSize(Long.parseLong(clientData[3]));
                    simulationOutput.setTamperingError(Boolean.parseBoolean(clientData[4]));
                    simulationOutput.setIntegrityError(Boolean.parseBoolean(clientData[5]));
                    simulationOutput.setCheckIntegrityOverhead(Long.parseLong(clientData[6]));
                    Logger.info(Main.class, "Join result table size: " + simulationOutput.getResultTableSize(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Join result table number of tuples: " + simulationOutput.getResultTableTuples(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Check integrity + clean overhead: " + simulationOutput.getCheckIntegrityOverhead() + " ms", LogLevel.REQUIRED);

                    Logger.info(Main.class, "Number of generated twins " + storageServerL.get("name") + ": " + simulationOutput.getNumberOfTwinsL(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Number of generated twins " + storageServerR.get("name") + ": " + simulationOutput.getNumberOfTwinsR(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Number of generated markers: " + simulationOutput.getNumberOfMarkers(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Total number of control tuples: " + simulationOutput.getTotalControlTuples(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Control tuples size: " + simulationOutput.getControlTuplesSize() + " bytes", LogLevel.REQUIRED);

                    String[] creationOverheadData1 = regularTempReader.readLine().split(":");
                    String[] creationOverheadData2 = regularTempReader.readLine().split(":");
                    assert creationOverheadData1.length == 2 && creationOverheadData2.length == 2;
                    if (creationOverheadData1[0].equals(storageServerL.get("name"))) {
                        assert creationOverheadData2[0].equals(storageServerR.get("name"));
                        simulationOutput.setCreationOverheadL(Long.parseLong(creationOverheadData1[1]));
                        simulationOutput.setCreationOverheadR(Long.parseLong(creationOverheadData2[1]));
                    } else {
                        assert creationOverheadData1[0].equals(storageServerR.get("name"));
                        assert creationOverheadData2[0].equals(storageServerL.get("name"));
                        simulationOutput.setCreationOverheadL(Long.parseLong(creationOverheadData2[1]));
                        simulationOutput.setCreationOverheadR(Long.parseLong(creationOverheadData1[1]));
                    }
                    Logger.info(Main.class, "Creation overhead " + storageServerL.get("name") + ": " + simulationOutput.getCreationOverheadL() + " ms", LogLevel.REQUIRED);
                    Logger.info(Main.class, "Creation overhead " + storageServerR.get("name") + ": " + simulationOutput.getCreationOverheadR() + " ms\n", LogLevel.REQUIRED);
                }

                twinTempReader.close();
                clientTempReader.close();
                regularTempReader.close();
                twinTempFile.deleteOnExit();
                clientTempFile.deleteOnExit();
                regularTempFile.deleteOnExit();

                if (!overwriteStatisticsFile) {
                    @SuppressWarnings("unchecked")
                    List<SimulationOutput> oldSimulations = CustomJsonParser.readJsonFile(simulationConfigFile.getStatisticsTwinsFilePath(), List.class);
                    assert oldSimulations != null;
                    simulationOutputs.addAll(oldSimulations);
                }

                CustomJsonParser.writeJsonFile(simulationOutputs, simulationConfigFile.getStatisticsTwinsFilePath());
            } catch (IOException e) {
                Logger.err(Main.class, e);
                twinTempFile.deleteOnExit();
                clientTempFile.deleteOnExit();
                regularTempFile.deleteOnExit();
                System.exit(1);
            }
        } else {
            BufferedReader occurrencesTempReader, clientTempReader;
            try {
                occurrencesTempReader = new BufferedReader(new FileReader(QueryHandler.OCCURRENCES_TEMP_FILE_PATH));
                clientTempReader = new BufferedReader(new FileReader(ResultService.CLIENT_TEMP_FILE_PATH));

                for (int i = 0; i < simulationConfigFile.getNumberOfRuns(); i++) {
                    SimulationOutput simulationOutput = simulationOutputs.get(i);
                    simulationOutput.setRunId(i + 1);
                    Logger.info(Main.class, "------------------- RUN " + (i + 1) + " -------------------", LogLevel.REQUIRED);
                    Logger.info(Main.class, "Elapsed time: " + simulationOutput.getElapsedTime() + " ms", LogLevel.REQUIRED);

                    String[] occurrences1 = occurrencesTempReader.readLine().split(":");
                    String[] occurrences2 = occurrencesTempReader.readLine().split(":");
                    assert occurrences1.length == 5 && occurrences2.length == 5;
                    if (occurrences1[0].equals(storageServerL.get("name"))) {
                        assert occurrences2[0].equals(storageServerR.get("name"));
                        simulationOutput.setOccurrencesControlsL(Integer.parseInt(occurrences1[1]));
                        simulationOutput.setOccurrencesControlsR(Integer.parseInt(occurrences2[1]));
                        simulationOutput.setMaxNumberOfOccurrencesL(Integer.parseInt(occurrences1[2]));
                        simulationOutput.setMaxNumberOfOccurrencesR(Integer.parseInt(occurrences2[2]));
                        simulationOutput.setTableLsize(occurrences1[3]);
                        simulationOutput.setTableRsize(occurrences2[3]);
                        simulationOutput.setCreationOverheadL(Long.parseLong(occurrences1[4]));
                        simulationOutput.setCreationOverheadR(Long.parseLong(occurrences2[4]));
                    } else {
                        assert occurrences1[0].equals(storageServerR.get("name"));
                        assert occurrences2[0].equals(storageServerL.get("name"));
                        simulationOutput.setOccurrencesControlsL(Integer.parseInt(occurrences2[1]));
                        simulationOutput.setOccurrencesControlsR(Integer.parseInt(occurrences1[1]));
                        simulationOutput.setMaxNumberOfOccurrencesL(Integer.parseInt(occurrences2[2]));
                        simulationOutput.setMaxNumberOfOccurrencesR(Integer.parseInt(occurrences1[2]));
                        simulationOutput.setTableLsize(occurrences2[3]);
                        simulationOutput.setTableRsize(occurrences1[3]);
                        simulationOutput.setCreationOverheadL(Long.parseLong(occurrences2[4]));
                        simulationOutput.setCreationOverheadR(Long.parseLong(occurrences1[4]));
                    }
                    Logger.info(Main.class, "Storage server " +  storageServerL.get("name") + " table size: " + simulationOutput.getTableLsize(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Storage server " +  storageServerR.get("name") + " table size: " + simulationOutput.getTableRsize(), LogLevel.REQUIRED);

                    String[] clientData = clientTempReader.readLine().split(":");
                    assert clientData.length == 7;
                    simulationOutput.setResultTableSize(clientData[0]);
                    simulationOutput.setResultTableTuples(Integer.parseInt(clientData[1]));
                    if (!clientData[2].equals("null")) {
                        simulationOutput.setTotalControlTuples(Integer.parseInt(clientData[2]));
                    }
                    simulationOutput.setControlTuplesSize(Long.parseLong(clientData[3]));
                    simulationOutput.setTamperingError(Boolean.parseBoolean(clientData[4]));
                    simulationOutput.setIntegrityError(Boolean.parseBoolean(clientData[5]));
                    simulationOutput.setCheckIntegrityOverhead(Long.parseLong(clientData[6]));
                    Logger.info(Main.class, "Join result table size: " + simulationOutput.getResultTableSize(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Join result table number of tuples: " + simulationOutput.getResultTableTuples(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Check integrity + clean overhead: " + simulationOutput.getCheckIntegrityOverhead() + " ms", LogLevel.REQUIRED);

                    Logger.info(Main.class, "Number of values for occurrences control in " + storageServerL.get("name") + ": " + simulationOutput.getOccurrencesControlsL(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Number of values for occurrences control in " + storageServerR.get("name") + ": " + simulationOutput.getOccurrencesControlsR(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Number of generated markers " + simulationOutput.getNumberOfMarkers(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Total number of control tuples: " + simulationOutput.getTotalControlTuples(), LogLevel.REQUIRED);
                    Logger.info(Main.class, "Control tuples size: " + simulationOutput.getControlTuplesSize() + " bytes", LogLevel.REQUIRED);
                    Logger.info(Main.class, "Creation overhead " + storageServerL.get("name") + ": " + simulationOutput.getCreationOverheadL() + " ms", LogLevel.REQUIRED);
                    Logger.info(Main.class, "Creation overhead " + storageServerR.get("name") + ": " + simulationOutput.getCreationOverheadR() + " ms\n", LogLevel.REQUIRED);
                }

                occurrencesTempReader.close();
                clientTempReader.close();
                occurrencesTempFile.deleteOnExit();
                clientTempFile.deleteOnExit();

                if (!overwriteStatisticsFile) {
                    @SuppressWarnings("unchecked")
                    List<SimulationOutput> oldSimulations = CustomJsonParser.readJsonFile(simulationConfigFile.getStatisticsOccurrencesFilePath(), List.class);
                    assert oldSimulations != null;
                    simulationOutputs.addAll(oldSimulations);
                }

                CustomJsonParser.writeJsonFile(simulationOutputs, simulationConfigFile.getStatisticsOccurrencesFilePath());
            } catch (IOException e) {
                Logger.err(Main.class, e);
                occurrencesTempFile.deleteOnExit();
                clientTempFile.deleteOnExit();
                System.exit(1);
            }
        }
        System.exit(0);
    }

    private static Process startProcess(Map<String, String> config, String jarFile, String mainClass, String dbName) {
        assert config != null && jarFile != null && mainClass != null;
        assert !jarFile.isBlank() && !mainClass.isBlank();
        assert config.containsKey("name") &&
                config.containsKey("port") &&
                config.containsKey("ipAddr") &&
                config.containsKey("outputFilePath");

        List<String> cmd;
        if (dbName == null) {
            cmd = new ArrayList<>(10) {{
                add("java");
                add("-ea");
                add(MAX_CSP_HEAP_SIZE);
                add(STACK_THREAD_SIZE);
                add("-cp");
                add(jarFile);
                add(mainClass);
                add(config.get("name"));
                add(config.get("port"));
                add(config.get("ipAddr"));
            }};
        } else {
            cmd = new ArrayList<>(11) {{
                add("java");
                add("-ea");
                add(MAX_STORAGE_HEAP_SIZE);
                add(STACK_THREAD_SIZE);
                add("-cp");
                add(jarFile);
                add(mainClass);
                add(config.get("name"));
                add(config.get("port"));
                add(dbName);
                add(config.get("ipAddr"));
            }};
        }

        ProcessBuilder processBuilder = new ProcessBuilder(cmd);
        processBuilder.inheritIO();
        processBuilder.redirectOutput(new File(config.get("outputFilePath")));
        Process process = null;
        try {
            process = processBuilder.start();
        } catch (IOException e) {
            Logger.err(Main.class, e);
        }
        return process;
    }
}