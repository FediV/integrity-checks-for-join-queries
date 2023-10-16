package communication;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.Map;
import java.util.List;

/**
 * The ClientConfigFile class.
 * Class representation of the client.config.json file.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ClientConfigFile {
    private Map<String, QueryMessage> queryMessages;
    private TwinCondition twinCondition;
    private List<String> workers;
    private boolean isSemiJoin;
    private int minNumberOfMarkers;
    private int maxNumberOfMarkers;
    private int numberOfMarkers;
    private int replicationFactor;
    private boolean useOccurrences;

    public Map<String, QueryMessage> getQueryMessages() {
        return queryMessages;
    }

    public void setQueryMessages(Map<String, QueryMessage> queryMessages) {
        this.queryMessages = queryMessages;
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

    public int getMaxNumberOfMarkers() {
        return maxNumberOfMarkers;
    }

    public void setMaxNumberOfMarkers(int maxNumberOfMarkers) {
        this.maxNumberOfMarkers = maxNumberOfMarkers;
    }

    public int getMinNumberOfMarkers() {
        return minNumberOfMarkers;
    }

    public void setMinNumberOfMarkers(int minNumberOfMarkers) {
        this.minNumberOfMarkers = minNumberOfMarkers;
    }

    public int getNumberOfMarkers() {
        return numberOfMarkers;
    }

    public void setNumberOfMarkers(int numberOfMarkers) {
        this.numberOfMarkers = numberOfMarkers;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public ClientConfigFile() {}

    public ClientConfigFile(Map<String, QueryMessage> queryMessages, TwinCondition twinCondition, List<String> workers,
                            boolean isSemiJoin, boolean useOccurences, int minNumberOfMarkers, int numberOfMarkers,
                            int maxNumberOfMarkers, int replicationFactor) {
        this.queryMessages = queryMessages;
        this.twinCondition = twinCondition;
        this.workers = workers;
        this.isSemiJoin = isSemiJoin;
        this.useOccurrences = useOccurences;
        this.minNumberOfMarkers = minNumberOfMarkers;
        this.numberOfMarkers = numberOfMarkers;
        this.maxNumberOfMarkers = maxNumberOfMarkers;
        this.replicationFactor = replicationFactor;
    }
}
