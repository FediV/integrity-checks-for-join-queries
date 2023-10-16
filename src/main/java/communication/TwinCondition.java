package communication;

import java.util.List;

/**
 * The TwinCondition class.
 * Class representation of the client.config.json > twinCondition field.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class TwinCondition {
	private List<String> values;
	private float pTwin;

	public TwinCondition() {}

	public TwinCondition(List<String> values, float pTwin) {
		this.values = values;
		this.pTwin = pTwin;
	}

	public TwinCondition(List<String> values) {
		this.values = values;
		this.pTwin = -1.0f;
	}

	public TwinCondition(float pTwin) {
		this.values = null;
		this.pTwin = pTwin;
	}

	public float getPTwin() {
		return pTwin;
	}

	public void setPTwin(float pTwin) {
		this.pTwin = pTwin;
	}

	public List<String> getValues() {
		return values;
	}

	public void setValues(List<String> values) {
		this.values = values;
	}
}
