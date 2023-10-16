package server;

import com.fasterxml.jackson.core.JsonProcessingException;
import communication.DistributedJoinQueryMessage;
import communication.JoinQueryMessage;
import communication.ResultMessage;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import simulation.SimulationConfigFile;
import utility.CustomJsonParser;
import utility.LogLevel;
import utility.Logger;
import utility.Network;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * The ComputationalServer class.
 * Server functionality for the CSP.
 * Default address: 127.0.0.1:1339
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class ComputationalServer {

	private static final String LOCAL_HOST = "localhost";
	private static final String SIMULATION_CONFIG_FILE_PATH = "./config/simulation.config.json";
	private String ipAddr;
	private int port;
	private HttpServer server;

	// SERVER VARIABLES FOR ALL SERVICES
	private static String name;

	private static final ClientConfig clientConfig = new ClientConfig();
	private static final Client client = ClientBuilder.newClient(clientConfig);

	private static String clientAddr;
	private static int clientPort;
	private static final String JOIN_RESULT_PATH = "send-result";

	private static final ConcurrentHashMap<String, JoinQueryMessage> relationBuffer = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, DistributedJoinQueryMessage> distributedMessageBuffer = new ConcurrentHashMap<>();
	private static final int MAX_SIZE = 1000;
	private static SimulationConfigFile simulationConfigFile;
	private static final Random RNG = new Random();

	private ComputationalServer(String serverName, String ipAddr, int port) {
		ComputationalServer.setName(serverName);
		this.setIpAddr(ipAddr);
		this.setPort(port);

		ComputationalServer.simulationConfigFile = CustomJsonParser.readJsonFile(SIMULATION_CONFIG_FILE_PATH, SimulationConfigFile.class);
		assert ComputationalServer.simulationConfigFile != null;

		Map<String, String> clientParams = ComputationalServer.simulationConfigFile.getClient();
		assert clientParams != null;
		ComputationalServer.clientAddr = clientParams.get("ipAddr");
		ComputationalServer.clientPort = Integer.parseInt(clientParams.get("port"));
	}

	// Handle REST requests
	public static void main(String[] args) {
		Logger.info(ComputationalServer.class, Arrays.toString(args), LogLevel.REQUIRED);
		ComputationalServer computationalServer = null;
		switch (args.length) {
			case 2 -> computationalServer = new ComputationalServer(args[0], "127.0.0.1", Integer.parseInt(args[1]));
			case 3 -> computationalServer = new ComputationalServer(args[0], args[2], Integer.parseInt(args[1]));
			default -> {
				Logger.err(ComputationalServer.class, "Not enough arguments to start the storage server:\n" +
						"Please use: <serverName> <port> {<serverAddress>}");
				System.exit(1);
			}
		}

		try {
			computationalServer.startServer();
			computationalServer.stopServer(5);
		} catch (IOException e) {
			try {
				computationalServer.setIpAddr(ComputationalServer.LOCAL_HOST);
				computationalServer.startServer();
				computationalServer.stopServer(10);
			} catch (IOException ex) {
				Logger.err(ComputationalServer.class, "Unable to create an HTTP server on " + computationalServer.getPort());
				ex.printStackTrace();
			}
		}
	}

	private void startServer() throws IOException {
		ResourceConfig config = new ResourceConfig().packages("server.services");
		this.server = GrizzlyHttpServerFactory.createHttpServer(URI.create(Network.createURL(this.getIpAddr(), this.getPort())), config);
		this.server.start();
		Logger.info(this, "Server " + ComputationalServer.getName() + " running!", LogLevel.REQUIRED);
		Logger.info(this, "Server running at: " + this, LogLevel.REQUIRED);
	}

	private void stopServer(int delay) throws IOException {
		Logger.info(this, "Press return to stop...", LogLevel.REQUIRED);
		System.in.read();
		Logger.info(this, "Stopping server...", LogLevel.REQUIRED);
		server.shutdown(delay, TimeUnit.MILLISECONDS);
		Logger.info(this, "Server stopped", LogLevel.REQUIRED);
		System.exit(0);
	}

	public static void join() {
		assert ComputationalServer.relationBuffer.keySet().size() == 2;
		ArrayList<JoinQueryMessage> messages = new ArrayList<>(ComputationalServer.relationBuffer.values());
		assert messages.size() == 2;
		List<Pair<String, String>> firstRelation = messages.get(0).getRelation();
		List<Pair<String, String>> secondRelation = messages.get(1).getRelation();
		int firsJoinIndex = messages.get(0).getJoinAttribute();
		int secondJoinIndex = messages.get(1).getJoinAttribute();

		List<Triple<String, String, String>> joinResult = ComputationalServer.doJoin(
				firstRelation, secondRelation,
				firsJoinIndex, secondJoinIndex
		);

		ComputationalServer.sendJoinResult(joinResult);
		relationBuffer.clear();
	}

	private static void sendJoinResult(List<Triple<String, String, String>> joinResult) {
		try {
			String message;
			int numberOfFragments = (int) Math.ceil(joinResult.size() / (double) MAX_SIZE);
			for (int i = 0, j = 0; i < joinResult.size(); i += MAX_SIZE, j++) {
				int end = Math.min(joinResult.size(), i + MAX_SIZE);
				message = CustomJsonParser.serializeObject(new ResultMessage(joinResult.subList(i, end), j, numberOfFragments));
				Network.post(ComputationalServer.client,
						Network.createURL(
								ComputationalServer.clientAddr,
								ComputationalServer.clientPort,
								ComputationalServer.JOIN_RESULT_PATH), message
				);
			}
		} catch (JsonProcessingException e) {
			Logger.err(ComputationalServer.class, e);
		}
	}

	private static List<Triple<String, String, String>> doJoin(List<Pair<String, String>> firstRelation,
	                                                           List<Pair<String, String>> secondRelation,
	                                                           int firstJoinIndex, int secondJoinIndex) {
		List<Triple<String, String, String>> result = new ArrayList<>();
		float tamperingProbability = ComputationalServer.simulationConfigFile.getTamperingProbability();
		float lazyProbability = ComputationalServer.simulationConfigFile.getLazyProbability();
		assert tamperingProbability >= 0.0 && tamperingProbability <= 1.0 &&
				lazyProbability >= 0.0 && lazyProbability <= 1.0;

		if (firstJoinIndex == 0) {
			Map<String, List<Pair<String, String>>> firstRelationMap = firstRelation.stream()
					.collect(Collectors.groupingBy(Pair::getLeft));
			if (secondJoinIndex == 0) {
				for (Pair<String, String> t : secondRelation) {
					String joinAttribute = t.getLeft();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= lazyProbability) {
								if (RNG.nextDouble() < tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									result.add(ComputationalServer.tamper(joinAttribute, p.getRight(), t.getRight()));
								} else {
									result.add(Triple.of(joinAttribute, p.getRight(), t.getRight()));
								}
							}
						}
					}
				}
			} else {
				for (Pair<String, String> t : secondRelation) {
					String joinAttribute = t.getRight();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= lazyProbability) {
								if (RNG.nextDouble() < tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									result.add(ComputationalServer.tamper(joinAttribute, p.getRight(), t.getLeft()));
								} else {
									result.add(Triple.of(joinAttribute, p.getRight(), t.getLeft()));
								}
							}
						}
					}
				}
			}
		} else {
			Map<String, List<Pair<String, String>>> firstRelationMap = firstRelation.stream()
					.collect(Collectors.groupingBy(Pair::getRight));
			if (secondJoinIndex == 0) {
				for (Pair<String, String> t : secondRelation) {
					String joinAttribute = t.getLeft();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= lazyProbability) {
								if (RNG.nextDouble() < tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									result.add(ComputationalServer.tamper(joinAttribute, p.getLeft(), t.getRight()));
								} else {
									result.add(Triple.of(joinAttribute, p.getLeft(), t.getRight()));
								}
							}
						}
					}
				}
			} else {
				for (Pair<String, String> t : secondRelation) {
					String joinAttribute = t.getRight();
					List<Pair<String, String>> joinCandidates = firstRelationMap.get(joinAttribute);
					if (joinCandidates != null) {
						for (Pair<String, String> p : joinCandidates) {
							if (RNG.nextDouble() >= lazyProbability) {
								if (RNG.nextDouble() < tamperingProbability) {
									// Tampering with probability 'tamperingProbability'
									result.add(ComputationalServer.tamper(joinAttribute, p.getLeft(), t.getLeft()));
								} else {
									result.add(Triple.of(joinAttribute, p.getLeft(), t.getLeft()));
								}
							}
						}
					}
				}
			}
		}
		return result;
	}

	public static void distributedJoin() {
		Set<String> senderNames = ComputationalServer.distributedMessageBuffer.keySet();
		assert senderNames.size() == 2;

		ArrayList<DistributedJoinQueryMessage> messages;

		// SPECIAL CASE FOR SIMULATION PURPOSES: KEEP ORDER: FIRST 'L' THEN 'R'
		if (senderNames.contains("L") && senderNames.contains("R")) {
			messages = new ArrayList<>() {{
				add(ComputationalServer.distributedMessageBuffer.get("L"));
				add(ComputationalServer.distributedMessageBuffer.get("R"));
			}};
		} else {
			messages = new ArrayList<>(ComputationalServer.distributedMessageBuffer.values());
		}
		assert messages.size() == 2;
		JoinQueryMessage firstJoinMessage = messages.get(0).getJoinQueryMessage();
		JoinQueryMessage secondJoinMessage = messages.get(1).getJoinQueryMessage();
		List<String> workers = messages.get(0).getWorkers();
		assert workers.equals(messages.get(1).getWorkers());
		List<Pair<String, String>> firstRelation = firstJoinMessage.getRelation();
		List<Pair<String, String>> secondRelation = secondJoinMessage.getRelation();
		int firstJoinIndex = firstJoinMessage.getJoinAttribute();
		int secondJoinIndex = secondJoinMessage.getJoinAttribute();

		try {
			List<List<Triple<String, String, String>>> joinMap = ComputationalServer.mapJoin(workers,
					firstRelation, secondRelation, firstJoinIndex, secondJoinIndex);
			List<Triple<String, String, String>> joinResult = ComputationalServer.reduceJoin(joinMap);
			ComputationalServer.sendJoinResult(joinResult);
		} catch (InterruptedException e) {
			Logger.err(ComputationalServer.class, e);
		} finally {
			distributedMessageBuffer.clear();
		}
	}

	private static List<List<Triple<String, String, String>>> mapJoin(List<String> workerIds,
	                                                                  List<Pair<String, String>> firstRelation,
	                                                                  List<Pair<String, String>> secondRelation,
	                                                                  int firsJoinIndex, int secondJoinIndex)
			throws InterruptedException {
		int numOfWorkers = workerIds.size();
		Logger.info(ComputationalServer.class, "Number of workers: " + numOfWorkers, LogLevel.COMPLETE);
		Map<String, Map<Character, List<Pair<String, String>>>> assignments = new HashMap<>();

		for (String workerId : workerIds) {
			assignments.computeIfAbsent(workerId, k -> new HashMap<>());
			assignments.get(workerId).computeIfAbsent('L', k -> new ArrayList<>());
			assignments.get(workerId).computeIfAbsent('R', k -> new ArrayList<>());
			assignments.get(workerId).get('L').addAll(firstRelation.stream().filter(p ->
					ComputationalServer.assignTupleToWorker(p.getLeft(), numOfWorkers).equals(workerId)).toList());
			assignments.get(workerId).get('R').addAll(secondRelation.stream().filter(p ->
					ComputationalServer.assignTupleToWorker(p.getLeft(), numOfWorkers).equals(workerId)).toList());
		}

		Logger.info(ComputationalServer.class, assignments.toString(), LogLevel.REQUIRED);
		float tamperingProbability = ComputationalServer.simulationConfigFile.getTamperingProbability();
		float lazyProbability = ComputationalServer.simulationConfigFile.getLazyProbability();
		float trustedWorkers = ComputationalServer.simulationConfigFile.getTrustedWorkers();

		assert trustedWorkers >= 0.0 && trustedWorkers <= 1.0;
		int numOfTrusted = Math.round(numOfWorkers * trustedWorkers);
		Worker[] workers = new Worker[numOfWorkers];
		List<List<Triple<String, String, String>>> joinResults = new ArrayList<>(numOfWorkers);

		assert !assignments.isEmpty();
		for (int i = 0; i < numOfWorkers; i++) {
			String workerId = workerIds.get(i);
			if (!assignments.get(workerId).get('L').isEmpty() && !assignments.get(workerId).get('R').isEmpty()) {
				if (i < numOfTrusted) {
					workers[i] = new Worker(
							workerId,
							assignments.get(workerId).get('L'),
							assignments.get(workerId).get('R'),
							firsJoinIndex,
							secondJoinIndex,
							0,
							0
					);
				} else {
					workers[i] = new Worker(
							workerId,
							assignments.get(workerId).get('L'),
							assignments.get(workerId).get('R'),
							firsJoinIndex,
							secondJoinIndex,
							tamperingProbability,
							lazyProbability
					);
				}
				workers[i].start();
			}
		}
		for (int i = 0; i < numOfWorkers; i++) {
			if (workers[i] != null) {
				workers[i].join();
			}
		}
		for (var worker : workers) {
			if (worker != null) {
				assert !worker.isAlive() && worker.getResult() != null :
						"Thread: " + worker.getWorkerId() + " terminated abnormally!";
				joinResults.add(worker.getResult());
			}
		}
		return joinResults;
	}

	public static String assignTupleToWorker(String joinAttrHash, int numOfWorkers) {
		return "worker_" + ((joinAttrHash.hashCode() & 0x7fffffff) % numOfWorkers);
	}

	private static List<Triple<String, String, String>> reduceJoin(List<List<Triple<String, String, String>>> joinMap) {
		return joinMap.parallelStream().reduce((x, y) -> {
			List<Triple<String, String, String>> l = new ArrayList<>(x);
			l.addAll(y);
			return l;
		}).orElse(new ArrayList<>());
	}

	protected static Triple<String, String, String> tamper(String first, String second, String third) {
		String tamperString = Integer.toString(RNG.nextInt());
		switch (RNG.nextInt(3)) {
			case 0 -> {
				return Triple.of(first + tamperString, second, third);
			}
			case 1 -> {
				return Triple.of(first, second + tamperString, third);
			}
			case 2 -> {
				return Triple.of(first, second, third + tamperString);
			}
			default -> Logger.err(ComputationalServer.class, "Something wrong with the RNG...");
		}
		return null;
	}

	public static int addRelation(String serverName, JoinQueryMessage message) {
		if (message != null) {
			relationBuffer.put(serverName, message);
		}
		return relationBuffer.size();
	}

	public static int addDistributedMessage(String serverName, DistributedJoinQueryMessage message) {
		if (message != null) {
			distributedMessageBuffer.put(serverName, message);
		}
		return distributedMessageBuffer.size();
	}

	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}

	public void setPort(int port) {
		if (port >= 0 && port <= 65535) {
			this.port = port;
		} else {
			this.port = 1339;
			Logger.err(this, "Invalid port number: " + port + " | port set to default (1339)");
		}
	}

	public String getIpAddr() {
		return this.ipAddr;
	}

	public int getPort() {
		return this.port;
	}

	public static String getName() {
		return ComputationalServer.name;
	}

	public static void setName(String name) {
		ComputationalServer.name = name;
	}

	@Override
	public String toString() {
		return Network.createURL(this.getIpAddr(), this.getPort());
	}
}
