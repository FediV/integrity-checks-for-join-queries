package communication;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.tuple.Triple;

import java.util.List;

/**
 * The ResultMessage class.
 * Payload for the POST request to /send-result.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ResultMessage {
    private List<Triple<String,String,String>> partialResult;
    private int id;
    private int numberOfFragments;

    public List<Triple<String, String, String>> getPartialResult() {
        return partialResult;
    }

    public void setPartialResult(List<Triple<String, String, String>> partialResult) {
        this.partialResult = partialResult;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNumberOfFragments() {
        return numberOfFragments;
    }

    public void setNumberOfFragments(int numberOfFragments) {
        this.numberOfFragments = numberOfFragments;
    }

    public ResultMessage() {
    }

    public ResultMessage(List<Triple<String, String, String>> partialResult, int id, int numberOfFragments) {
        this.partialResult = partialResult;
        this.id = id;
        this.numberOfFragments = numberOfFragments;
    }

    @Override
    public String toString() {
        return "ResultMessage{" +
                "id=" + id +
                ", numberOfFragments=" + numberOfFragments +
                '}';
    }
}
