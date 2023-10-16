package simulation;

import java.util.Map;

/**
 * The SimulationConfigFile class.
 * Class representation of the simulation.config.json file.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class SimulationConfigFile {
    private Map<String,String> storageServer1;
    private Map<String,String> storageServer2;
    private Map<String,String> computationalServer;
    private Map<String,String> client;
    private int numberOfRuns;
    private int loggerLevel;
    private float tamperingProbability;
    private float lazyProbability;
    private float trustedWorkers;
    private boolean overwriteStatisticsFile;
    private String statisticsTwinsFilePath;
    private String statisticsOccurrencesFilePath;

    public Map<String, String> getStorageServer1() {
        return storageServer1;
    }

    public void setStorageServer1(Map<String, String> storageServer1) {
        this.storageServer1 = storageServer1;
    }

    public Map<String, String> getStorageServer2() {
        return storageServer2;
    }

    public void setStorageServer2(Map<String, String> storageServer2) {
        this.storageServer2 = storageServer2;
    }

    public Map<String, String> getComputationalServer() {
        return computationalServer;
    }

    public void setComputationalServer(Map<String, String> computationalServer) {
        this.computationalServer = computationalServer;
    }

    public Map<String, String> getClient() {
        return client;
    }

    public void setClient(Map<String, String> client) {
        this.client = client;
    }

    public int getNumberOfRuns() {
        return numberOfRuns;
    }

    public void setNumberOfRuns(int numberOfRuns) {
        this.numberOfRuns = numberOfRuns;
    }

    public int getLoggerLevel() {
        return loggerLevel;
    }

    public void setLoggerLevel(int loggerLevel) {
        this.loggerLevel = loggerLevel;
    }

    public boolean getOverwriteStatisticsFile() {
        return overwriteStatisticsFile;
    }

    public void setOverwriteStatisticsFile(boolean overwriteStatisticsFile) {
        this.overwriteStatisticsFile = overwriteStatisticsFile;
    }

    public String getStatisticsTwinsFilePath() {
        return statisticsTwinsFilePath;
    }

    public void setStatisticsTwinsFilePath(String statisticsTwinsFilePath) {
        this.statisticsTwinsFilePath = statisticsTwinsFilePath;
    }

    public String getStatisticsOccurrencesFilePath() {
        return statisticsOccurrencesFilePath;
    }

    public void setStatisticsOccurrencesFilePath(String statisticsOccurrencesFilePath) {
        this.statisticsOccurrencesFilePath = statisticsOccurrencesFilePath;
    }

    public float getTamperingProbability() {
        return tamperingProbability;
    }

    public void setTamperingProbability(float tamperingProbability) {
        this.tamperingProbability = tamperingProbability;
    }

    public float getLazyProbability() {
        return lazyProbability;
    }

    public void setLazyProbability(float lazyProbability) {
        this.lazyProbability = lazyProbability;
    }

    public float getTrustedWorkers() {
        return trustedWorkers;
    }

    public void setTrustedWorkers(float trustedWorkers) {
        this.trustedWorkers = trustedWorkers;
    }

    public SimulationConfigFile() {
    }

    public SimulationConfigFile(Map<String, String> storageServer1, Map<String, String> storageServer2,
                                Map<String, String> computationalServer, Map<String, String> client,
                                int numberOfRuns, int loggerLevel, boolean overwriteStatisticsFile,
                                String statisticsTwinsFilePath, String statisticsOccurrencesFilePath,
                                float tamperingProbability, float lazyProbability, float trustedWorkers) {
        this.storageServer1 = storageServer1;
        this.storageServer2 = storageServer2;
        this.computationalServer = computationalServer;
        this.client = client;
        this.numberOfRuns = numberOfRuns;
        this.loggerLevel = loggerLevel;
        this.overwriteStatisticsFile = overwriteStatisticsFile;
        this.statisticsTwinsFilePath = statisticsTwinsFilePath;
        this.statisticsOccurrencesFilePath = statisticsOccurrencesFilePath;
        this.tamperingProbability = tamperingProbability;
        this.lazyProbability = lazyProbability;
        this.trustedWorkers = trustedWorkers;
    }
}
