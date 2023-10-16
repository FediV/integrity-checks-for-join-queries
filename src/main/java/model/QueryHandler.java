package model;

import com.fasterxml.jackson.core.JsonProcessingException;
import communication.DistributedJoinQueryMessage;
import communication.JoinQueryMessage;
import communication.QueryMessage;
import communication.TwinCondition;
import org.apache.commons.lang3.tuple.Pair;
import server.ComputationalServer;
import server.StorageServer;
import utility.*;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The QueryHandler class.
 * Handle the query request from the client.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class QueryHandler {
	public static final String TWIN_TEMP_FILE_PATH = "twin_temp.txt";
	public static final String OCCURRENCES_TEMP_FILE_PATH = "occurrences_temp.txt";
	public static final String REGULAR_TEMP_FILE_PATH = "regular_temp.txt";
	private final int minNumberOfMarkers;
	private final int maxNumberOfMarkers;
	private final int numberOfMarkers;
	private final String query;
	private final String queryFilter;
	private final int replicationFactor;
	private final TwinCondition twinCondition;
	private Connection connect;
	private final List<String> workers;
	private final int numOfWorkers;
	private final int minSaltLength;
	private final String joinAttribute;
	private final SecretKey aesKey;
	private final byte[] initVector;
	private final SecretKey sha3Key;
	private static final int JOIN_ATTR_INDEX = 0;
	private static final String SELECT_ALL_QUERY_PREFIX = "SELECT * FROM ";
	private final boolean semiJoin;
	private int numberOfSalts;

	private final static Object LOCK = new Object();

	// COUNTERS
	private int markerCounter;
	private final Map<Integer, Integer> twinCounter;

	public QueryHandler(QueryMessage queryMessage) {
		this.minNumberOfMarkers = queryMessage.getMinNumberOfMarkers();
		this.maxNumberOfMarkers = queryMessage.getMaxNumberOfMarkers();
		this.numberOfMarkers = queryMessage.getNumberOfMarkers();
		this.query = QueryHandler.SELECT_ALL_QUERY_PREFIX + StorageServer.getTableName() + " " + queryMessage.getQueryFilter();
		this.queryFilter = queryMessage.getQueryFilter();
		this.replicationFactor = queryMessage.getReplicationFactor();
		this.twinCondition = queryMessage.getTwinCondition();
		this.workers = queryMessage.getWorkers();
		this.numOfWorkers = this.workers.size();
		this.minSaltLength = queryMessage.getMinSaltLength();
		this.connect = ModelUtils.startDBConnection(StorageServer.getDbUrl());
		this.markerCounter = 0;
		this.twinCounter = new HashMap<>();
		this.joinAttribute = StorageServer.getJoinAttribute();
		byte[] decodedAesKey = Base64.getDecoder().decode(queryMessage.getAESkey());
		this.aesKey = new SecretKeySpec(decodedAesKey, 0, decodedAesKey.length, Encryption.AES);
		byte[] decodedShaKey = Base64.getDecoder().decode(queryMessage.getSHAkey());
		this.sha3Key = new SecretKeySpec(decodedShaKey, 0, decodedShaKey.length, Encryption.HMAC_SHA3_256);
		this.initVector = queryMessage.getAESinitVector();
		this.semiJoin = queryMessage.getIsSemiJoin();
		this.numberOfSalts = 0;
	}

	public void setupJoinWithOccurrences() {
		ResultSet queryResult = ModelUtils.queryDB(this.query, this.connect);
		long startTime = System.nanoTime();

		String occurencesQuery;
		if (this.queryFilter.isBlank()) {
			occurencesQuery = "SELECT " + this.joinAttribute + ", COUNT (*)" +
					" FROM " + StorageServer.getTableName() + " GROUP BY " + this.joinAttribute;
		} else {
			occurencesQuery = "SELECT " + this.joinAttribute + ", COUNT (*)" +
					" FROM " + StorageServer.getTableName() + " " + this.queryFilter + " GROUP BY " + this.joinAttribute;
		}

		ResultSet occurrencesResult = ModelUtils.queryDB(occurencesQuery, this.connect);

		HashMap<String,Integer> joinValuesOccurrences = new HashMap<>();
		try {
			while (occurrencesResult.next()) {
				joinValuesOccurrences.put(occurrencesResult.getString(1), occurrencesResult.getInt(2));
			}
		} catch (SQLException e) {
			Logger.err(this, e);
		}

		Set<Pair<String, String>> markers = this.generateMarkers(
				this.numberOfMarkers,
				this.minNumberOfMarkers,
				this.maxNumberOfMarkers
		);

		List<Pair<String, String>> partialResult = new ArrayList<>();
		try {
			if (this.semiJoin) {
				while (queryResult.next()) {
					partialResult.add(Pair.of(StorageServer.addServerName(
									queryResult.getString("ID")),
							queryResult.getString(this.joinAttribute)
							+ "|" + joinValuesOccurrences.get(queryResult.getString(this.joinAttribute))));
				}
			} else {
				while (queryResult.next()) {
					partialResult.add(Pair.of(StorageServer.addServerName(
									queryResult.getString("ID")),
							queryResult.getString(this.joinAttribute) + "|" + queryResult.getString(3)
					         + "|" + joinValuesOccurrences.get(queryResult.getString(this.joinAttribute))));
				}
			}
			queryResult.close();
		} catch (Exception e) {
			Logger.err(this, e);
		}

		int nmax = 0;
		if (this.numOfWorkers >= 2) { // For distributed join with occurrences only
			nmax = Collections.max(joinValuesOccurrences.values());
			Network.post(StorageServer.getClient(),
					Network.createURL(
							StorageServer.getClientAddr(),
							StorageServer.getClientPort(),
							StorageServer.getNmaxSaltsClientPath()), nmax);
		}

		if (markers != null) {
			partialResult.addAll(markers);
		}

		Logger.info(this, partialResult.toString(), LogLevel.REQUIRED);

		if (this.numOfWorkers >= 2) { // For distributed join with occurrences only
			synchronized (QueryHandler.LOCK) {
				try {
					while (this.numberOfSalts == 0) {
						QueryHandler.LOCK.wait();
					}
					assert this.numberOfSalts > 0 || this.numberOfSalts == -1;
					Logger.info(this, "Number of salts: " + this.numberOfSalts, LogLevel.REQUIRED);
					if (this.numberOfSalts == -1) {
						Logger.info(this, "Detected n:m join, skipping s&b!", LogLevel.REQUIRED);
					} else {
						partialResult = this.addSalts(partialResult, this.numberOfSalts, this.numOfWorkers, nmax);
						Logger.info(this, "Partial result after s&b: " + partialResult, LogLevel.REQUIRED);
					}
				} catch (InterruptedException e) {
					Logger.err(this, e);
				}
			}
		}

		partialResult = partialResult.stream().map(p -> Pair.of(
						Encryption.encrypt(Encryption.getPrefix(p.getRight(), "|"), this.aesKey, this.initVector),
						Encryption.encrypt(p.getLeft() + "|" + Encryption.removeSalt(p.getRight()),
								this.aesKey, this.initVector)))
				.toList();

		// <Storage server name>:<number of values with occurrences greater than two>:<max occurrence>:<table size>:<elapsed_time_ms>
		long endTime = System.nanoTime();
		long elapsed = (endTime - startTime) / 1000000;
		try {
			FileWriter myWriter = new FileWriter(OCCURRENCES_TEMP_FILE_PATH, true);
			myWriter.write(StorageServer.getName() + ":"
					+ joinValuesOccurrences.values().stream().filter( o -> o > 1).count() + ":"
					+ Collections.max(joinValuesOccurrences.values()) + ":"
					+ Statistics.getTableSize(StorageServer.getDbUrl(), connect) + ":"
					+ elapsed + "\n");
			myWriter.close();
		} catch (IOException e) {
			Logger.err(QueryHandler.class, e);
		}

		this.sendPartialResult(partialResult);
	}

	private void sendPartialResult(List<Pair<String, String>> partialResult) {
		try {
			ModelUtils.closeDBConnection(this.connect);
			this.connect = null;
			String message;
			if (this.numOfWorkers >= 2) {
				message = CustomJsonParser.serializeObject(new DistributedJoinQueryMessage(
						new JoinQueryMessage(partialResult, QueryHandler.JOIN_ATTR_INDEX, StorageServer.getName()),
						this.workers));
				Network.post(StorageServer.getClient(),
						Network.createURL(
								StorageServer.getCspAddr(),
								StorageServer.getCspPort(),
								StorageServer.getDistributedJoinCspPath()), message
				);
			} else {
				message = CustomJsonParser.serializeObject(new JoinQueryMessage(partialResult,
						QueryHandler.JOIN_ATTR_INDEX, StorageServer.getName()));
				Network.post(StorageServer.getClient(),
						Network.createURL(
								StorageServer.getCspAddr(),
								StorageServer.getCspPort(),
								StorageServer.getJoinCspPath()), message
				);
			}
		} catch (JsonProcessingException e) {
			Logger.err(this, e);
		}
	}

	public void setupRegularJoin() {
		ResultSet queryResult = ModelUtils.queryDB(this.query, this.connect);
		long startTime = System.nanoTime();

		Set<Pair<String, String>> markers = this.generateMarkers(
				this.numberOfMarkers,
				this.minNumberOfMarkers,
				this.maxNumberOfMarkers
		);
		Set<Pair<String, String>> twins = this.generateTwins(
				this.twinCondition
		);

		List<Pair<String, String>> partialResult = new ArrayList<>();
		try {
			if (this.semiJoin) {
				while (queryResult.next()) {
					partialResult.add(Pair.of(StorageServer.addServerName(
									queryResult.getString("ID")),
							queryResult.getString(this.joinAttribute)));
				}
			} else {
				while (queryResult.next()) {
					partialResult.add(Pair.of(StorageServer.addServerName(
									queryResult.getString("ID")),
							queryResult.getString(this.joinAttribute) + "|" + queryResult.getString(3)));
				}
			}
			queryResult.close();
		} catch (Exception e) {
			Logger.err(this, e);
		}

		if (markers != null) {
			partialResult.addAll(markers);
		}
		if (twins != null) {
			partialResult.addAll(twins);
		}

		Logger.info(this, partialResult.toString(), LogLevel.COMPLETE);

		if (this.semiJoin) {
			partialResult = partialResult.stream().map(p -> Pair.of(
							Encryption.encrypt(p.getRight(), this.aesKey, this.initVector),
							Encryption.encrypt(p.getLeft() + "|" + Encryption.removeSalt(p.getRight()),
									this.aesKey, this.initVector)))
							.toList();
		} else {
			partialResult = partialResult.stream().map(p -> Pair.of(
							Encryption.encrypt(Encryption.getPrefix(p.getRight(), "|"), this.aesKey, this.initVector),
							Encryption.encrypt(p.getLeft() + "|" + Encryption.removeSalt(p.getRight()),
									this.aesKey, this.initVector)))
							.toList();
		}

		// <Storage server name>:<elapsed_time_ms>
		long endTime = System.nanoTime();
		long elapsed = (endTime - startTime) / 1000000;
		try {
			FileWriter myWriter = new FileWriter(REGULAR_TEMP_FILE_PATH, true);
			myWriter.write(StorageServer.getName() + ":" + elapsed + "\n");
			myWriter.close();
		} catch (IOException e) {
			Logger.err(QueryHandler.class, e);
		}

		this.sendPartialResult(partialResult);
	}

	private Set<Pair<String, String>> generateMarkers(int n, int nMin, int nMax) {
		if (nMin < 0 || nMax < nMin || n < nMin) {
			Logger.err(this, "Parameters' values are incorrect!");
			return null;
		}
		if (this.workers == null || this.workers.isEmpty()) {
			Logger.err(this, "Workers' list is empty!");
			return null;
		}
		if (n == 0) {
			Logger.warn(this, "The number of markers to generate is 0");
			return null;
		}
		int l = this.numOfWorkers;
		if (l * nMax < n) {
			Logger.err(this, "Cannot generate " + n + " markers with " + l + " workers and nMax = " + nMax);
			return null;
		}

		Set<Pair<String, String>> M = new HashSet<>();
		int spare = n - (nMin * l);
		Map<String, Integer> numMarkers = new HashMap<>();
		for (String worker : this.workers) {
			numMarkers.put(worker, 0);
		}

		do {
			String m = this.getNextMarker();
			String w = ComputationalServer.assignTupleToWorker(Objects.requireNonNull(
					Encryption.encrypt(m, this.aesKey, this.initVector)), l);
			if ((numMarkers.get(w) < nMin) || ((numMarkers.get(w) < nMax && spare > 0))) {
				numMarkers.put(w, numMarkers.get(w) + 1);
				if (numMarkers.get(w) > nMin) {
					spare--;
				}
				Logger.info(this, "Marker: " + m + " assigned to worker: " + w, LogLevel.COMPLETE);
				M.add(Pair.of((-1 * this.markerCounter) + "_m", m));
			}
		} while (M.size() != n);

		Logger.info(this, "Generated markers (" + M.size() + "): " + M, LogLevel.REQUIRED);

		return M;
	}

	private String getNextMarker() {
		return "marker_" + this.markerCounter++;
	}

	private Set<Pair<String, String>> generateTwins(TwinCondition twinCondition) {
		// 1. Get twinCondition.values
		// 2. If values null o empty ->
		// 2.1 Get twinCondition.pTwin
		// 2.2 Generate condition as: h(t[I]) % Math.floor(1/pTwin) == 0
		//      h hash function, t[I] stands for a tuple in the relation 'tableName' projected on the JOIN
		//      attribute I.
		// 3. If values defined ->
		// 3.1 Select from the relation 'tableName' where t[I] == at least one value in values
		//      t[I] stands for a tuple in the relation 'tableName' projected on the JOIN attribute I.
		if (this.workers == null || this.workers.isEmpty()) {
			Logger.err(this, "Workers' list is empty!");
			return null;
		}
		Set<Pair<String, String>> T_bar = new HashSet<>();

		try {
			ResultSet result = ModelUtils.queryDB(this.query, this.connect);
			List<String> values = twinCondition.getValues();
			Map<Integer, String> T = new HashMap<>();
			if (values == null || values.isEmpty()) {
				float pTwin = twinCondition.getPTwin();
				BigInteger invPTwin = new BigInteger(String.valueOf((int) Math.floor(1 / pTwin)));
				Logger.info(QueryHandler.class, invPTwin.toString(), LogLevel.REQUIRED);
				if (this.semiJoin) {
					while (result.next()) {
						String t = result.getString(this.joinAttribute);
						if ((Objects.requireNonNull(Encryption.hash(t, this.sha3Key)).mod(invPTwin).equals(BigInteger.ZERO))) {
							T.put(result.getInt("ID"), t);
						}
					}
				} else {
					while (result.next()) {
						String t = result.getString(this.joinAttribute);
						if ((Objects.requireNonNull(Encryption.hash(t, this.sha3Key)).mod(invPTwin).equals(BigInteger.ZERO))) {
							T.put(result.getInt("ID"), t + "|" + result.getString(3));
						}
					}
				}
			} else {
				if (this.semiJoin) {
					while (result.next()) {
						String t = result.getString(this.joinAttribute);
						if (values.contains(t)) {
							T.put(result.getInt("ID"), t);
						}
					}
				} else {
					while (result.next()) {
						String t = result.getString(this.joinAttribute);
						if (values.contains(t)) {
							T.put(result.getInt("ID"), t + "|" + result.getString(3));
						}
					}
				}
			}
			Set<String> assigned = new HashSet<>();
			if (this.semiJoin) {
				for (Map.Entry<Integer, String> t : T.entrySet()) {
					int twinExactCounter = this.twinCounter.get(t.getKey()) == null ? 0 : this.twinCounter.get(t.getKey());
					for (int i = 1; i < this.replicationFactor; i++) {
						// Twin separation property
						String deterministicSalt = this.generateSalt(this.minSaltLength, twinExactCounter);
						String candidateTwinJoinAttr = t.getValue() + deterministicSalt;
						int counterSalt = 0;
						String w = ComputationalServer.assignTupleToWorker(
								Objects.requireNonNull(
										Encryption.encrypt(candidateTwinJoinAttr, this.aesKey, this.initVector)
								),
								this.numOfWorkers
						);
						Logger.info(this, "Original twinned tuple: " + candidateTwinJoinAttr + " assigned to worker: " +
								w, LogLevel.COMPLETE);
						assigned.add(w);

						while(assigned.size() < this.numOfWorkers && assigned.contains(w)) {
							assigned.add(w);
							candidateTwinJoinAttr = t.getValue() + deterministicSalt + "_" + counterSalt;
							counterSalt = (counterSalt + 1) % Integer.MAX_VALUE;
							w = ComputationalServer.assignTupleToWorker(
									Objects.requireNonNull(
											Encryption.encrypt(candidateTwinJoinAttr, this.aesKey, this.initVector)
									),
									this.numOfWorkers
							);
						}
						Logger.info(this, "Twin tuple: " + candidateTwinJoinAttr + " assigned to worker: " +
								w, LogLevel.COMPLETE);
						assigned.clear();
						T_bar.add(Pair.of(StorageServer.addServerName(String.valueOf(t.getKey())), candidateTwinJoinAttr));
						twinExactCounter++;
					}
					this.twinCounter.put(t.getKey(), twinExactCounter);
				}
			} else {
				for (Map.Entry<Integer, String> t : T.entrySet()) {
					int twinExactCounter = this.twinCounter.get(t.getKey()) == null ? 0 : this.twinCounter.get(t.getKey());
					String[] splittedValue = t.getValue().split("\\|");
					assert splittedValue.length >= 2;
					for (int i = 1; i < this.replicationFactor; i++) {
						// Twin separation property
						String deterministicSalt = this.generateSalt(this.minSaltLength, twinExactCounter);
						String candidateTwinJoinAttr = splittedValue[0] + deterministicSalt;
						int counterSalt = 0;
						String w = ComputationalServer.assignTupleToWorker(
								Objects.requireNonNull(
										Encryption.encrypt(candidateTwinJoinAttr, this.aesKey, this.initVector)
								),
								this.numOfWorkers
						);
						Logger.info(this, "Original twinned tuple: " + candidateTwinJoinAttr + " assigned to worker: " +
								w, LogLevel.COMPLETE);
						assigned.add(w);

						while(assigned.size() < this.numOfWorkers && assigned.contains(w)) {
							assigned.add(w);
							candidateTwinJoinAttr = splittedValue[0] + deterministicSalt + "_" + counterSalt;
							counterSalt = (counterSalt + 1) % Integer.MAX_VALUE;
							w = ComputationalServer.assignTupleToWorker(
									Objects.requireNonNull(
											Encryption.encrypt(candidateTwinJoinAttr, this.aesKey, this.initVector)
									),
									this.numOfWorkers
							);
						}
						Logger.info(this, "Twin tuple: " + candidateTwinJoinAttr + " assigned to worker: " +
								w, LogLevel.COMPLETE);
						assigned.clear();
						T_bar.add(Pair.of(StorageServer.addServerName(String.valueOf(t.getKey())),
								candidateTwinJoinAttr + "|" + splittedValue[1]));
						twinExactCounter++;
					}
					this.twinCounter.put(t.getKey(), twinExactCounter);
				}
			}
			result.close();
		} catch (SQLException e) {
			Logger.err(this, e);
		}

		Logger.info(this, "Generated twins (" + T_bar.size() + "): " + T_bar, LogLevel.REQUIRED);
		// <Storage server name>:<number of twins>:<table size>
		try {
			FileWriter myWriter = new FileWriter(TWIN_TEMP_FILE_PATH, true);
			myWriter.write(StorageServer.getName() + ":" + T_bar.size() + ":"
					+ Statistics.getTableSize(StorageServer.getDbUrl(), connect) + "\n");
			myWriter.close();
		} catch (IOException e) {
			Logger.err(QueryHandler.class, e);
		}
		return T_bar;
	}

	public String semiJoin(List<String> tids) throws SQLException, JsonProcessingException {
		if (this.connect == null) {
			this.connect = ModelUtils.startDBConnection(StorageServer.getDbUrl());
		}
		StringBuilder query = new StringBuilder(QueryHandler.SELECT_ALL_QUERY_PREFIX +
				StorageServer.getTableName() +
				" WHERE ID IN (");
		for (String tid: tids) {
			query.append(tid).append(',');
		}
		query.delete(query.length() - 1, query.length());
		query.append(')');
		Logger.info(this, query.toString(), LogLevel.REQUIRED);
		ResultSet resultSet = ModelUtils.queryDB(query.toString(), this.connect);
		Map<String, Pair<String, String>> resultTuples = new HashMap<>();
		while (resultSet.next()) {
			resultTuples.put(
					resultSet.getString(1),
					Pair.of(resultSet.getString(2),
							resultSet.getString(3))
			);
		}
		resultSet.close();
		ModelUtils.closeDBConnection(this.connect);
		this.connect = null;
		return CustomJsonParser.serializeObject(resultTuples);
	}

	private String generateSalt(int minLength, int twinExactCounter) {
		if (minLength <= 0) {
			Logger.err(this, "Salt length must be > 0!");
			return null;
		}

		String salt = "" + twinExactCounter;
		if (salt.length() < minLength - 1) {
			StringBuilder saltBuilder = new StringBuilder(salt);
			for (int i = salt.length(); i < minLength - 1; i++) {
				saltBuilder.insert(0, "0");
			}
			salt = saltBuilder.toString();
		}
		return "-" + salt;
	}

	public void setNumberOfSalts(int numberOfSalts) {
		synchronized (QueryHandler.LOCK) {
			this.numberOfSalts = numberOfSalts;
			LOCK.notifyAll();
		}
	}

	private List<Pair<String, String>> addSalts(List<Pair<String, String>> partialResult, int s, int numOfWorkers, int nmax) {
		s = Math.min(s, numOfWorkers);
		int b = (nmax <= numOfWorkers) ? 1 : (int) Math.ceil(nmax / (double) s); // Single bucket size
		Logger.info(this, "Using number of salts s: " + s + " | Using buckets' size b: " + b, LogLevel.COMPLETE);
		List<Pair<String, String>> saltedPartialResult = new ArrayList<>(partialResult.size());
		HashSet<String> assigned = new HashSet<>();
		String joinAttrValue;
		String w = null;
		if (nmax == 1) { // Join side 1
			// Replicate each tuple of side 1 s times
			for (Pair<String, String> tuple: partialResult) {
				if (tuple.getRight().contains("|")) { // Not a marker
					joinAttrValue = Encryption.getPrefix(tuple.getRight(), "|");
					int saltCounter = 0;
					String saltedJoinAttr = null;
					for (int i = 0; i < s; i++) {
						if (assigned.isEmpty()) {
							saltedJoinAttr = null;
							w = ComputationalServer.assignTupleToWorker(
									Objects.requireNonNull(
											Encryption.encrypt(joinAttrValue, this.aesKey, this.initVector)
									),
									numOfWorkers
							);
						} else {
							while (assigned.size() < numOfWorkers && assigned.contains(w)) {
								saltedJoinAttr = joinAttrValue + "%%" + (saltCounter++);
								w = ComputationalServer.assignTupleToWorker(
										Objects.requireNonNull(
												Encryption.encrypt(saltedJoinAttr, this.aesKey, this.initVector)
										),
										numOfWorkers
								);
							}
						}
						// Tuple/pair format: (<tid>,<joinAttr>|<otherAttr>|<occ>) e.g.: (1-L,Jerrie|69|1)
						if (saltedJoinAttr != null && tuple.getRight().contains("|")) {
							saltedPartialResult.add(Pair.of(tuple.getLeft(), saltedJoinAttr + tuple.getRight().substring(tuple.getRight().indexOf("|"))));
						} else { // Unmodified tuple
							saltedPartialResult.add(tuple);
						}

						assigned.add(w);
						if (assigned.size() == s) {
							saltCounter = 0;
							assigned.clear();
						}
					}
					assigned.clear();
				} else {
					saltedPartialResult.add(tuple);
				}
			}
		} else { // Join side n
			Map<String, List<Pair<String, String>>> joinBucketsMap = partialResult.stream().collect(Collectors.groupingBy(t -> Encryption.getPrefix(t.getRight(), "|")));
			Map<String, List<Pair<String, String>>> saltedBucketsMap = new HashMap<>();
			Logger.info(this, joinBucketsMap.toString(), LogLevel.REQUIRED);
			int saltCounter = 0;
			int joinAttrListSize;
			List<Pair<String, String>> joinAttrList;
			String saltedJoinAttr;
			for (Map.Entry<String, List<Pair<String, String>>> joinAttrBucketMap: joinBucketsMap.entrySet()) {
				joinAttrValue = joinAttrBucketMap.getKey();
				joinAttrList = joinAttrBucketMap.getValue();
				joinAttrListSize = joinAttrList.size();
				saltedJoinAttr = null;
				for (int i = 0; i < joinAttrListSize; i += b) {
					if (i + b <= joinAttrListSize) {
						if (i == 0) { // First bucket, no need to add a salt
							w = ComputationalServer.assignTupleToWorker(
									Objects.requireNonNull(
											Encryption.encrypt(joinAttrValue, this.aesKey, this.initVector)
									),
									numOfWorkers
							);
							assigned.add(w);
							assert !saltedBucketsMap.containsKey(joinAttrValue);
							List<Pair<String, String>> associatedTuples = new ArrayList<>(joinAttrList.subList(i, i + b));
							saltedBucketsMap.put(joinAttrValue, associatedTuples);
						} else {
							do {
								saltedJoinAttr = joinAttrValue + "%%" + (saltCounter++);
								w = ComputationalServer.assignTupleToWorker(
										Objects.requireNonNull(
												Encryption.encrypt(saltedJoinAttr, this.aesKey, this.initVector)
										),
										numOfWorkers
								);
							} while (assigned.contains(w) && assigned.size() < numOfWorkers);
							assigned.add(w);
							List<Pair<String, String>> associatedTuples;
							if (saltedBucketsMap.containsKey(saltedJoinAttr)) {
								associatedTuples = saltedBucketsMap.get(saltedJoinAttr);
								associatedTuples.addAll(joinAttrList.subList(i, i + b));
							} else {
								associatedTuples = new ArrayList<>(joinAttrList.subList(i, i + b));
							}
							saltedBucketsMap.put(saltedJoinAttr, associatedTuples);

							if (assigned.size() == numOfWorkers) {
								assigned.clear();
							}
						}
					} else {
						int numOfDummies = b - (joinAttrListSize % b);
						if (!joinAttrValue.contains("marker")) {
							if (saltedJoinAttr == null) {
								saltedJoinAttr = joinAttrValue + "%%" + saltCounter;
							}
							while (assigned.contains(w) && assigned.size() < numOfWorkers) {
								saltedJoinAttr = joinAttrValue + "%%" + (saltCounter++);
								w = ComputationalServer.assignTupleToWorker(
										Objects.requireNonNull(
												Encryption.encrypt(saltedJoinAttr, this.aesKey, this.initVector)
										),
										numOfWorkers
								);
							}
						} else {
							saltedJoinAttr = joinAttrValue;
						}
						List<Pair<String, String>> associatedTuples;
						if (saltedBucketsMap.containsKey(saltedJoinAttr)) {
							associatedTuples = saltedBucketsMap.get(saltedJoinAttr);
							associatedTuples.addAll(joinAttrList.subList(i, i + (b - numOfDummies)));
						} else {
							associatedTuples = new ArrayList<>(joinAttrList.subList(i, i + (b - numOfDummies)));
						}
						associatedTuples.addAll(this.generateDummyTuples(saltedJoinAttr, numOfDummies));
						saltedBucketsMap.put(saltedJoinAttr, associatedTuples);
					}
				}
				assigned.clear();
				saltCounter = 0;
			}
			// Reconstruct saltedPartialResult from saltedBuckets
			for (Map.Entry<String, List<Pair<String, String>>> saltedBucket: saltedBucketsMap.entrySet()) {
				for (Pair<String, String> tuple: saltedBucket.getValue()) {
					if (tuple.getRight().contains("|")) {
						saltedPartialResult.add(Pair.of(tuple.getLeft(), saltedBucket.getKey() + tuple.getRight().substring(tuple.getRight().indexOf("|"))));
					} else { // Marker
						saltedPartialResult.add(tuple);
					}
					Logger.info(this, "I: " + saltedBucket.getKey() + " | Assigned to worker: " +
							ComputationalServer.assignTupleToWorker(Objects.requireNonNull(
									Encryption.encrypt(saltedBucket.getKey(), this.aesKey, this.initVector)
							),
							numOfWorkers), LogLevel.COMPLETE
					);
				}
			}
		}
		return saltedPartialResult;
	}

	private List<Pair<String, String>> generateDummyTuples(String joinAttrValue, int n) {
		// Tuple/pair format: (<tid>,<joinAttr>|<otherAttr>|<occ>) e.g.: (1-L,Jerrie|69|1)
		assert n > 0 && joinAttrValue != null;
		return Collections.nCopies(n, Pair.of("dummy", joinAttrValue + "|dummy|dummy"));
	}
}
