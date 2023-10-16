package client;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Map;
import java.util.Set;

/**
 * The IntegrityViolationException class.
 * Custom runtime exception for the integrity checks.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class IntegrityViolationException extends RuntimeException {
	public IntegrityViolationException(String msg) {
		super("INTEGRITY ERROR: " + msg);
	}

	public IntegrityViolationException() {
		super("INTEGRITY ERROR!");
	}

	public IntegrityViolationException(Set<Pair<String, String>> remainingMarkers,
	                                   Set<Triple<String, String, String>> remainingTwins) {
		super(
				"INTEGRITY ERROR!\n" +
				"Missing markers: " + remainingMarkers + "\n" +
				"Missing twins: " + remainingTwins
		);
	}

	public IntegrityViolationException(Set<Pair<String, String>> remainingMarkers,
									   Map<String, Integer> joinValuesWithWrongOccurrences) {
		super(
				"INTEGRITY ERROR!\n" +
				"Missing markers: " + remainingMarkers + "\n" +
				"Join values with missing occurrences: " + joinValuesWithWrongOccurrences
		);
	}
}

