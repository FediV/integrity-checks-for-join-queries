package communication;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.List;

/**
 * The DistributedJoinQueryMessage class.
 * Payload for the POST request to /distributed-join.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class DistributedJoinQueryMessage {
	private JoinQueryMessage joinQueryMessage;
	private List<String> workers;

	public List<String> getWorkers() {
		return workers;
	}

	public void setWorkers(List<String> workers) {
		this.workers = workers;
	}

	public JoinQueryMessage getJoinQueryMessage() {
		return joinQueryMessage;
	}

	public void setJoinQueryMessage(JoinQueryMessage joinQueryMessage) {
		this.joinQueryMessage = joinQueryMessage;
	}

	public DistributedJoinQueryMessage() {}

	public DistributedJoinQueryMessage(JoinQueryMessage joinQueryMessage, List<String> workers) {
		this.joinQueryMessage = joinQueryMessage;
		this.workers = workers;
	}

	@Override
	public String toString() {
		return "DistributedJoinQueryMessage{" +
				"joinQueryMessage=" + joinQueryMessage +
				", workers=" + workers +
				'}';
	}
}

