package server;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * The Worker class.
 * Class representation of the CSP's trusted or unknown workers.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class Worker extends Thread {
	private final String workerId;
	private final List<Pair<String, String>> firstRelation;
	private final List<Pair<String, String>> secondRelation;
	private final int firstJoinIndex;
	private final int secondJoinIndex;
	private final float tamperingProbability;
	private final float lazyProbability;
	private static final Random RNG = new Random();
	private List<Triple<String, String, String>> result;

	public Worker(String workerId,
	              List<Pair<String, String>> firstRelation,
	              List<Pair<String, String>> secondRelation,
	              int firsJoinIndex, int secondJoinIndex,
	              float tamperingProbability, float lazyProbability) {
		this.workerId = workerId;
		this.firstRelation = firstRelation;
		this.secondRelation = secondRelation;
		this.firstJoinIndex = firsJoinIndex;
		this.secondJoinIndex = secondJoinIndex;
		this.tamperingProbability = tamperingProbability;
		this.lazyProbability = lazyProbability;
	}

	@Override
	public void run() {
		this.result = new ArrayList<>();
		assert this.tamperingProbability >= 0.0 && this.tamperingProbability <= 1.0 &&
				this.lazyProbability >= 0.0 && this.lazyProbability <= 1.0;

		if (this.firstJoinIndex == 0) {
			Map<String, List<Pair<String, String>>> firstRelationMap = this.firstRelation.stream()
					.collect(Collectors.groupingBy(Pair::getLeft));
			if (this.secondJoinIndex == 0) {
				for (Pair<String, String> t : this.secondRelation) {
					String joinAttribute = t.getLeft();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= this.lazyProbability) {
								if (RNG.nextDouble() < this.tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									this.result.add(ComputationalServer.tamper(joinAttribute, p.getRight(), t.getRight()));
								} else {
									this.result.add(Triple.of(joinAttribute, p.getRight(), t.getRight()));
								}
							}
						}
					}
				}
			} else {
				for (Pair<String, String> t : this.secondRelation) {
					String joinAttribute = t.getRight();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= this.lazyProbability) {
								if (RNG.nextDouble() < this.tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									this.result.add(ComputationalServer.tamper(joinAttribute, p.getRight(), t.getLeft()));
								} else {
									this.result.add(Triple.of(joinAttribute, p.getRight(), t.getLeft()));
								}
							}
						}
					}
				}
			}
		} else {
			Map<String, List<Pair<String, String>>> firstRelationMap = this.firstRelation.stream()
					.collect(Collectors.groupingBy(Pair::getRight));
			if (this.secondJoinIndex == 0) {
				for (Pair<String, String> t : this.secondRelation) {
					String joinAttribute = t.getLeft();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= this.lazyProbability) {
								if (RNG.nextDouble() < this.tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									this.result.add(ComputationalServer.tamper(joinAttribute, p.getLeft(), t.getRight()));
								} else {
									this.result.add(Triple.of(joinAttribute, p.getLeft(), t.getRight()));
								}
							}
						}
					}
				}
			} else {
				for (Pair<String, String> t : this.secondRelation) {
					String joinAttribute = t.getRight();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= this.lazyProbability) {
								if (RNG.nextDouble() < this.tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									this.result.add(ComputationalServer.tamper(joinAttribute, p.getLeft(), t.getLeft()));
								} else {
									this.result.add(Triple.of(joinAttribute, p.getLeft(), t.getLeft()));
								}
							}
						}
					}
				}
			}
		}
	}

	public String getWorkerId() {
		return this.workerId;
	}

	public synchronized List<Triple<String, String, String>> getResult() {
		return this.result;
	}
}
