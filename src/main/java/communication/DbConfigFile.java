package communication;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.Map;

/**
 * The DbConfigFile class.
 * Class representation of the db.config.json file.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class DbConfigFile {
	private Map<String, DataConfigFile> dbs;
	private String joinAttribute;
	private Long seed;
	private String joinAttributeFilePath;

	public Map<String, DataConfigFile> getDbs() {
		return dbs;
	}

	public void setDbs(Map<String, DataConfigFile> dbs) {
		this.dbs = dbs;
	}

	public String getJoinAttribute() {
		return joinAttribute;
	}

	public void setJoinAttribute(String joinAttribute) {
		this.joinAttribute = joinAttribute;
	}

	public Long getSeed() {
		return seed;
	}

	public void setSeed(Long seed) {
		this.seed = seed;
	}

	public String getJoinAttributeFilePath() {
		return joinAttributeFilePath;
	}

	public void setJoinAttributeFilePath(String joinAttributeFilePath) {
		this.joinAttributeFilePath = joinAttributeFilePath;
	}

	public DbConfigFile(Map<String, DataConfigFile> dbs, String joinAttribute, Long seed, String joinAttributeFilePath) {
		this.dbs = dbs;
		this.joinAttribute = joinAttribute;
		this.seed = seed;
		this.joinAttributeFilePath = joinAttributeFilePath;
	}

	public DbConfigFile() {}

	@Override
	public String toString() {
		return "DbConfigFile{" +
				"dbs=" + dbs +
				", joinAttribute='" + joinAttribute + '\'' +
				", seed=" + seed +
				", joinAttributeFilePath='" + joinAttributeFilePath + '\'' +
				'}';
	}
}
