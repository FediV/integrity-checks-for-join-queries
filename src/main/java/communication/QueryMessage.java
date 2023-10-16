package communication;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.List;

/**
 * The QueryMessage class.
 * Payload for the POST request to /query/process.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class QueryMessage {
    private int minNumberOfMarkers;
    private int maxNumberOfMarkers;
    private int numberOfMarkers;
    private String queryFilter;
    private int replicationFactor;
    private TwinCondition twinCondition;
    private List<String> workers;
    private int minSaltLength;
    private String AESkey;
    private boolean useOccurrences;
    private boolean isSemiJoin;
    private byte[] AESinitVector;
    private String SHAkey;

    public int getMinNumberOfMarkers() {
        return minNumberOfMarkers;
    }

    public void setMinNumberOfMarkers(int minNumberOfMarkers) {
        this.minNumberOfMarkers = minNumberOfMarkers;
    }

    public int getMaxNumberOfMarkers() {
        return maxNumberOfMarkers;
    }

    public void setMaxNumberOfMarkers(int maxNumberOfMarkers) {
        this.maxNumberOfMarkers = maxNumberOfMarkers;
    }

    public int getNumberOfMarkers() {
        return numberOfMarkers;
    }

    public void setNumberOfMarkers(int numberOfMarkers) {
        this.numberOfMarkers = numberOfMarkers;
    }

    public String getQueryFilter() {
        return queryFilter;
    }

    public void setQueryFilter(String queryFilter) {
        this.queryFilter = queryFilter;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public TwinCondition getTwinCondition() {
        return twinCondition;
    }

    public void setTwinCondition(TwinCondition twinCondition) {
        this.twinCondition = twinCondition;
    }

    public List<String> getWorkers() {
        return workers;
    }

    public void setWorkers(List<String> workers) {
        this.workers = workers;
    }

    public int getMinSaltLength() {
        return minSaltLength;
    }

    public void setMinSaltLength(int minSaltLength) {
        this.minSaltLength = minSaltLength;
    }

    public byte[] getAESinitVector() {
        return AESinitVector;
    }

    public void setAESinitVector(byte[] AESinitVector) {
        this.AESinitVector = AESinitVector;
    }

    public String getAESkey() {
        return AESkey;
    }

    public void setAESkey(String AESkey) {
        this.AESkey = AESkey;
    }

    public String getSHAkey() {
        return SHAkey;
    }

    public void setSHAkey(String SHAkey) {
        this.SHAkey = SHAkey;
    }

    public boolean getIsSemiJoin() {
        return isSemiJoin;
    }

    public void setIsSemiJoin(boolean semiJoin) {
        this.isSemiJoin = semiJoin;
    }

    public boolean getUseOccurrences() {
        return useOccurrences;
    }

    public void setUseOccurrences(boolean useOccurrences) {
        this.useOccurrences = useOccurrences;
    }

    public QueryMessage() {}

    public QueryMessage(int minNumberOfMarkers, int maxNumberOfMarkers, int numberOfMarkers, String queryFilter,
                        int replicationFactor, TwinCondition twinCondition, List<String> workers, int minSaltLength,
                        String AESkey, byte[] initVector, String SHAkey, boolean isSemiJoin, boolean useOccurrences) {
        this.minNumberOfMarkers = minNumberOfMarkers;
        this.maxNumberOfMarkers = maxNumberOfMarkers;
        this.numberOfMarkers = numberOfMarkers;
        this.queryFilter = queryFilter;
        this.replicationFactor = replicationFactor;
        this.twinCondition = twinCondition;
        this.workers = workers;
        this.minSaltLength = minSaltLength;
        this.AESkey = AESkey;
        this.AESinitVector = initVector;
        this.SHAkey = SHAkey;
        this.isSemiJoin = isSemiJoin;
        this.useOccurrences = useOccurrences;
    }
}
