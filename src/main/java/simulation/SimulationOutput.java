package simulation;

/**
 * The SimulationOutput class.
 * Class representation of the simulation output from the Main class.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class SimulationOutput {
    private int runId;
    private long elapsedTime;
    private String tableLsize;
    private String tableRsize;
    private String resultTableSize;
    private int resultTableTuples;
    private int numberOfTwinsL;
    private int numberOfTwinsR;
    private int numberOfMarkers;
    private int maxNumberOfOccurrencesL;
    private int maxNumberOfOccurrencesR;
    private int occurrencesControlsL;
    private int occurrencesControlsR;
    private int totalControlTuples;
    private long controlTuplesSize;
    private boolean tamperingError;
    private boolean integrityError;
    private long creationOverheadL;
    private long creationOverheadR;
    private long checkIntegrityOverhead;

    public SimulationOutput() { }

    public SimulationOutput(int runId, long elapsedTime, String tableLsize, String tableRsize, String resultTableSize,
                            int resultTableTuples, int numberOfTwinsL, int numberOfTwinsR, int numberOfMarkers,
                            int maxNumberOfOccurrencesL, int maxNumberOfOccurrencesR,
                            int occurrencesControlsL, int occurrencesControlsR, int totalControlTuples,
                            long controlTuplesSize, boolean tamperingError, boolean integrityError, long creationOverheadL, long creationOverheadR,
                            long checkIntegrityOverhead) {
        this.runId = runId;
        this.elapsedTime = elapsedTime;
        this.tableLsize = tableLsize;
        this.tableRsize = tableRsize;
        this.resultTableSize = resultTableSize;
        this.resultTableTuples = resultTableTuples;
        this.numberOfTwinsL = numberOfTwinsL;
        this.numberOfTwinsR = numberOfTwinsR;
        this.numberOfMarkers = numberOfMarkers;
        this.maxNumberOfOccurrencesL = maxNumberOfOccurrencesL;
        this.maxNumberOfOccurrencesR = maxNumberOfOccurrencesR;
        this.occurrencesControlsL = occurrencesControlsL;
        this.occurrencesControlsR = occurrencesControlsR;
        this.totalControlTuples = totalControlTuples;
        this.controlTuplesSize = controlTuplesSize;
        this.tamperingError = tamperingError;
        this.integrityError = integrityError;
        this.creationOverheadL = creationOverheadL;
        this.creationOverheadR = creationOverheadR;
        this.checkIntegrityOverhead = checkIntegrityOverhead;
    }

    public int getRunId() {
        return runId;
    }

    public void setRunId(int runId) {
        this.runId = runId;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public String getTableLsize() {
        return tableLsize;
    }

    public void setTableLsize(String tableLsize) {
        this.tableLsize = tableLsize;
    }

    public String getTableRsize() {
        return tableRsize;
    }

    public void setTableRsize(String tableRsize) {
        this.tableRsize = tableRsize;
    }

    public String getResultTableSize() {
        return resultTableSize;
    }

    public void setResultTableSize(String resultTableSize) {
        this.resultTableSize = resultTableSize;
    }

    public int getResultTableTuples() {
        return resultTableTuples;
    }

    public void setResultTableTuples(int resultTableTuples) {
        this.resultTableTuples = resultTableTuples;
    }

    public int getNumberOfTwinsL() {
        return numberOfTwinsL;
    }

    public void setNumberOfTwinsL(int numberOfTwinsL) {
        this.numberOfTwinsL = numberOfTwinsL;
    }

    public int getNumberOfTwinsR() {
        return numberOfTwinsR;
    }

    public void setNumberOfTwinsR(int numberOfTwinsR) {
        this.numberOfTwinsR = numberOfTwinsR;
    }

    public int getNumberOfMarkers() {
        return numberOfMarkers;
    }

    public void setNumberOfMarkers(int numberOfMarkers) {
        this.numberOfMarkers = numberOfMarkers;
    }

    public int getMaxNumberOfOccurrencesL() {
        return maxNumberOfOccurrencesL;
    }

    public void setMaxNumberOfOccurrencesL(int maxNumberOfOccurrencesL) {
        this.maxNumberOfOccurrencesL = maxNumberOfOccurrencesL;
    }

    public int getMaxNumberOfOccurrencesR() {
        return maxNumberOfOccurrencesR;
    }

    public void setMaxNumberOfOccurrencesR(int maxNumberOfOccurrencesR) {
        this.maxNumberOfOccurrencesR = maxNumberOfOccurrencesR;
    }

    public int getOccurrencesControlsL() {
        return occurrencesControlsL;
    }

    public void setOccurrencesControlsL(int occurrencesControlsL) {
        this.occurrencesControlsL = occurrencesControlsL;
    }

    public int getOccurrencesControlsR() {
        return occurrencesControlsR;
    }

    public void setOccurrencesControlsR(int occurrencesControlsR) {
        this.occurrencesControlsR = occurrencesControlsR;
    }

    public int getTotalControlTuples() {
        return totalControlTuples;
    }

    public void setTotalControlTuples(int totalControlTuples) {
        this.totalControlTuples = totalControlTuples;
    }

    public long getControlTuplesSize() {
        return controlTuplesSize;
    }

    public void setControlTuplesSize(long controlTuplesSize) {
        this.controlTuplesSize = controlTuplesSize;
    }

    public boolean isTamperingError() {
        return tamperingError;
    }

    public void setTamperingError(boolean tamperingError) {
        this.tamperingError = tamperingError;
    }

    public boolean isIntegrityError() {
        return integrityError;
    }

    public void setIntegrityError(boolean integrityError) {
        this.integrityError = integrityError;
    }

    public long getCreationOverheadR() {
        return creationOverheadR;
    }

    public void setCreationOverheadR(long creationOverheadR) {
        this.creationOverheadR = creationOverheadR;
    }

    public long getCreationOverheadL() {
        return creationOverheadL;
    }

    public void setCreationOverheadL(long creationOverheadL) {
        this.creationOverheadL = creationOverheadL;
    }

    public long getCheckIntegrityOverhead() {
        return checkIntegrityOverhead;
    }

    public void setCheckIntegrityOverhead(long checkIntegrityOverhead) {
        this.checkIntegrityOverhead = checkIntegrityOverhead;
    }
}
