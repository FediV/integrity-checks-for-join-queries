package communication;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * The JoinQueryMessage class.
 * Payload for the POST request to /join.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class JoinQueryMessage {
    private List<Pair<String,String>> relation;
    private int joinAttribute;
    private String sender;

    public List<Pair<String, String>> getRelation() {
        return relation;
    }

    public void setRelation(List<Pair<String, String>> relation) {
        this.relation = relation;
    }

    public int getJoinAttribute() {
        return joinAttribute;
    }

    public void setJoinAttribute(int joinAttribute) {
        this.joinAttribute = joinAttribute;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public JoinQueryMessage() {
    }

    public JoinQueryMessage(List<Pair<String, String>> relation, int joinAttribute, String sender) {
        this.relation = relation;
        this.joinAttribute = joinAttribute;
        this.sender = sender;
    }

    @Override
    public String toString() {
        return "JoinQueryMessage{" +
                "relation=" + relation +
                ", joinAttribute='" + joinAttribute +
                ", sender=" + sender + '\'' +
                '}';
    }
}
