package client.services;

import client.IntegrityViolationException;
import client.TamperingException;
import client.RestClient;
import communication.QueryMessage;
import communication.ResultMessage;
import communication.TwinCondition;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import server.ComputationalServer;
import utility.*;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The ResultService class.
 * Exposed by: HttpServerThread
 * Endpoints:
 *  /send-result
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@Path("")
public class ResultService {
    public static final String CLIENT_TEMP_FILE_PATH = "client_temp.txt";
    private static final Set<Pair<String, String>> remainingMarkers = new HashSet<>();
    private static final Set<Triple<String, String, String>> remainingTwins = new HashSet<>();
    private static final Map<Integer, List<Triple<String, String, String>>> fragments = new ConcurrentHashMap<>();

    private static final Map<String, Integer> joinValuesWithWrongOccurrences = new HashMap<>();
    private static boolean useOccurences;
    private static boolean isSemiJoin;
    private static int totalControlTuples;

    private static long initialByteSize;
    private static long finalByteSize;
    private static long startTime;
    private static long elapsed;

    // COUNTERS
    private static int markerCounter;

    @Path("send-result")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response process(String joinResultSerialized) {
        FileWriter myWriter = null;
        try {
            myWriter = new FileWriter(CLIENT_TEMP_FILE_PATH, true);
            ResultMessage resultMessage = CustomJsonParser.deserializeObject(joinResultSerialized, ResultMessage.class);
            fragments.put(resultMessage.getId(), resultMessage.getPartialResult());

            Logger.info(ResultService.class, "Received fragment with id: " + resultMessage.getId() +
                    " (" + fragments.size() + "/" + resultMessage.getNumberOfFragments() + ")", LogLevel.COMPLETE);
            if (fragments.size() == resultMessage.getNumberOfFragments()) {
                Logger.info(ResultService.class, "All fragments received!", LogLevel.COMPLETE);
                List<Triple<String, String, String>> result = new ArrayList<>();
                fragments.values().forEach(result::addAll);

                List<Triple<String, String, String>> decryptedResult = new ArrayList<>(result.size());
                for (Triple<String, String, String> t : result) {
                    decryptedResult.add(ResultService.decryptTriple(t));
                }
                Logger.info(this, "Result BEFORE integrity checks and clean: " + decryptedResult, LogLevel.COMPLETE);

                ResultService.startTime = System.nanoTime();
                QueryMessage queryMessage = RestClient.getClientConfigFile().getQueryMessages().get("L");
                decryptedResult = decryptedResult.stream().map(Encryption::removeSalt).collect(Collectors.toList());

                ResultService.useOccurences = queryMessage.getUseOccurrences();
                ResultService.isSemiJoin = queryMessage.getIsSemiJoin();

                if (ResultService.useOccurences && queryMessage.getWorkers().size() > 1) {
                    decryptedResult = ResultService.removeSaltsAndDummy(decryptedResult);
                }

                int initialSize = decryptedResult.size();
                ResultService.initialByteSize = sizeOfTripleList(decryptedResult);

                if (ResultService.useOccurences) {
                    decryptedResult = ResultService.checkIntegrityWithOccurrences(decryptedResult, queryMessage);
                } else {
                    decryptedResult = ResultService.checkIntegrity(decryptedResult, queryMessage);
                }
                Logger.info(this, "Result AFTER integrity checks and clean: " + decryptedResult, LogLevel.COMPLETE);

                if (decryptedResult != null) {
                    ResultService.totalControlTuples = initialSize - decryptedResult.size();
                } else {
                    // <table size>:<table count>:<total control tuples>:<control tuples size>:<tampering error>:<integrity error>:<elapsed_time_ms>
                    myWriter.write("0 KB:0:" + ResultService.totalControlTuples + ":" + ResultService.finalByteSize +
                            ":false:true:" + ResultService.elapsed + "\n");
                    if (ResultService.useOccurences) {
                        throw new IntegrityViolationException(remainingMarkers, joinValuesWithWrongOccurrences);
                    } else {
                        throw new IntegrityViolationException(remainingMarkers, remainingTwins);
                    }
                }

                List<List<String>> resultRows = new ArrayList<>();
                if (ResultService.isSemiJoin) {
                    // Semi-join completion
                    // 1. Extract tid for L and R from join result
                    List<String> tidsL = decryptedResult.stream().map(t -> parseTid(t.getMiddle())).toList();
                    List<String> tidsR = decryptedResult.stream().map(t -> parseTid(t.getRight())).toList();

                    // 2. Make POST request to the storage servers to complete the
                    Response responseL, responseR;
                    responseL = Network.post(
                            RestClient.getClient(),
                            Network.createURL(RestClient.getStorageServerAddrL(), RestClient.getStorageServerPortL(), RestClient.getSemiJoinPath()),
                            tidsL
                    );
                    responseR = Network.post(
                            RestClient.getClient(),
                            Network.createURL(RestClient.getStorageServerAddrR(), RestClient.getStorageServerPortR(), RestClient.getSemiJoinPath()),
                            tidsR
                    );

                    // 3. Build final join result
                    Map<String, Pair<String, String>> resultL = CustomJsonParser.deserializeStringToStringPairMap(
                            responseL.readEntity(String.class)
                    );

                    Map<String, Pair<String, String>> resultR = CustomJsonParser.deserializeStringToStringPairMap(
                            responseR.readEntity(String.class)
                    );

                    for (Triple<String, String, String> t: decryptedResult) {
                        String tidL = ResultService.parseTid(t.getMiddle());
                        String tidR = ResultService.parseTid(t.getRight());
                        assert tidL != null && tidR != null;
                        Pair<String, String> infoL = resultL.get(tidL);
                        Pair<String, String> infoR = resultR.get(tidR);
                        assert infoL != null && infoR != null;
                        assert infoL.getLeft().equals(infoR.getLeft());
                        resultRows.add(new ArrayList<>() {{
                            add(tidL);
                            add(infoL.getLeft());
                            add(infoL.getRight());
                            add(tidR);
                            add(infoR.getLeft());
                            add(infoR.getRight());
                        }});
                    }
                } else {
                    String[] splittedL, splittedR;
                    for (Triple<String, String, String> t: decryptedResult) {
                        splittedL = t.getMiddle().split("\\|");
                        splittedR = t.getRight().split("\\|");
                        String tidL = ResultService.parseTid(t.getMiddle());
                        String tidR = ResultService.parseTid(t.getRight());
                        assert tidL != null && tidR != null;
                        assert splittedL[1].equals(splittedR[1]);
                        assert splittedL.length == 3 && splittedR.length == 3;

                        final String[] finalSplittedL = splittedL;
                        final String[] finalSplittedR = splittedR;

                        // Ignore occurrences if they are present,
                        // assertions ensure that everything is ok!
                        resultRows.add(new ArrayList<>() {{
                            add(tidL);
                            add(finalSplittedL[1]);
                            add(finalSplittedL[2]);
                            add(tidR);
                            add(finalSplittedR[1]);
                            add(finalSplittedR[2]);
                        }});
                    }
                }

                // The final result is saved in the client DB
                ResultService.insertDb(resultRows);

                // <table size>:<table count>:<total control tuples>:<control tuples size>:<tampering error>:<integrity error>:<elapsed_time_ms>
                myWriter.write(Statistics.getTableSize(RestClient.getDbUrl(), RestClient.connect)
                        + ":" + Statistics.getTableCount(RestClient.getDbUrl(), RestClient.getClientTableName(), RestClient.connect)
                        + ":" + ResultService.totalControlTuples
                        + ":" + ResultService.finalByteSize
                        + ":false:false:" + elapsed + "\n");

                fragments.clear();
                ModelUtils.closeDBConnection(RestClient.connect);
            }
        } catch (IOException e) {
            fragments.clear();
            ModelUtils.closeDBConnection(RestClient.connect);
            Logger.err(this, e);
            return Response.serverError().build();
        } catch (IntegrityViolationException e) {
            Logger.err(this, e, false);
            remainingMarkers.clear();
            remainingTwins.clear();
            joinValuesWithWrongOccurrences.clear();
            fragments.clear();
            ModelUtils.closeDBConnection(RestClient.connect);
            return Response.serverError().build();
        } catch (TamperingException e) {
            Logger.err(this, e, false);
            try {
                // <table size>:<table count>:<total control tuples>:<control tuples size>:<tampering error>:<integrity error>:<elapsed_time_ms>
                myWriter.write("0 KB:0:null:-1:true:false:-1\n");
            } catch (IOException ex) {
                fragments.clear();
                ModelUtils.closeDBConnection(RestClient.connect);
                Logger.err(this, e, false);
                return Response.serverError().build();
            }
            fragments.clear();
            ModelUtils.closeDBConnection(RestClient.connect);
            return Response.serverError().build();
        } finally {
            try {
                assert myWriter != null;
                myWriter.close();
            } catch (IOException e) {
                Logger.err(this, e);
            }
        }
        return Response.ok().build();
    }

    private static List<Triple<String, String, String>> removeSaltsAndDummy(List<Triple<String, String, String>> decryptedResult) {
        // Format example:
        // Regular tuple: (Jerrie%%1,1-L|Jerrie%%1|32|1,5-R|Jerrie%%1|Shock|5)
        // Marker: (marker_7,-8_m|marker_7,-8_m|marker_7)
        // Dummy tuple: (Jerrilee%%4,2-L|Jerrilee%%4|48|1,dummy|Jerrilee%%4|dummy|dummy)
        List<Triple<String, String, String>> result = new ArrayList<>();
        for (var t: decryptedResult) {
            if (!t.getMiddle().toLowerCase().contains("dummy|") && !t.getRight().toLowerCase().contains("dummy|")) {
                if (t.getLeft().contains("%%")) {
                    result.add(Triple.of(
                            t.getLeft().replaceFirst("%%\\d+", ""),
                            t.getMiddle().replaceFirst("%%\\d+", ""),
                            t.getRight().replaceFirst("%%\\d+", "")
                    ));
                } else {
                    result.add(t);
                }
            }
        }

        return result;
    }

    private static List<Triple<String, String, String>> checkIntegrityWithOccurrences(List<Triple<String, String, String>> J,
                                                                                      QueryMessage queryMessage) {
        int markerCounter = queryMessage.getNumberOfMarkers();

        ResultService.markerCounter = 0;
        Set<Pair<String, String>> M = Objects.requireNonNull(ResultService.generateMarkers(markerCounter,
                queryMessage.getMinNumberOfMarkers(),
                queryMessage.getMaxNumberOfMarkers(),
                queryMessage.getWorkers()));

        List<Triple<String, String, String>> result = new ArrayList<>(J);

        Map<String, MutablePair<Integer, Integer>> expectedOccurrences = new HashMap<>();
        Map<String, Pair<Integer, Integer>> frequencies = new HashMap<>();

        for (Triple<String, String, String> t : J) {
            String joinAttribute = t.getLeft();
            if (t.getLeft().startsWith("marker")) {
                markerCounter--;
                assert t.getMiddle().equals(t.getRight());
                M.remove(Pair.of(Encryption.getPrefix(t.getMiddle(), "|"), t.getLeft()));
                result.remove(t);
            } else {
                if (expectedOccurrences.containsKey(joinAttribute)) {
                    expectedOccurrences.get(joinAttribute).setRight(expectedOccurrences.get(joinAttribute).getRight() + 1);
                } else {
                    int freqL = Integer.parseInt(Encryption.getSuffix(t.getMiddle(), "|"));
                    int freqR = Integer.parseInt(Encryption.getSuffix(t.getRight(), "|"));
                    expectedOccurrences.put(joinAttribute, MutablePair.of(freqL * freqR, 1));
                    frequencies.put(joinAttribute, Pair.of(freqL, freqR));
                }
                result.remove(t);
                result.add(Triple.of(t.getLeft(), t.getMiddle().substring(0, t.getMiddle().lastIndexOf('|')), t.getRight().substring(0, t.getRight().lastIndexOf('|'))));
            }
        }

        for (Map.Entry<String, MutablePair<Integer,Integer>> e : expectedOccurrences.entrySet()) {
            int remainingOccurrences = e.getValue().getLeft() - e.getValue().getRight();
            if (remainingOccurrences != 0) {
                ResultService.joinValuesWithWrongOccurrences.put(e.getKey(), remainingOccurrences);
            }
        }

        long endTime = System.nanoTime();

        if (markerCounter > 0 || !ResultService.joinValuesWithWrongOccurrences.isEmpty()) {
            ResultService.remainingMarkers.addAll(M);
            ResultService.totalControlTuples =  J.size() - result.size() + markerCounter;
            ResultService.finalByteSize = ResultService.initialByteSize - sizeOfTripleList(result) + sizeOfMarkers(M)
                  +  joinValuesWithWrongOccurrences.entrySet().stream()
                                                    .mapToLong(t -> ((frequencies.get(t.getKey()).getLeft().toString().length() +
                                                                        frequencies.get(t.getKey()).getRight().toString().length()
                                                                        + 2L) * t.getValue())).sum();
            ResultService.elapsed = (endTime - ResultService.startTime) / 1000000;
            return null;
        }

        ResultService.finalByteSize = ResultService.initialByteSize - sizeOfTripleList(result);
        ResultService.elapsed = (endTime - ResultService.startTime) / 1000000;
        return result;
    }

    private static Triple<String, String, String> decryptTriple(Triple<String, String, String> t)
            throws TamperingException {
        try {
            return Triple.of(
                    Encryption.decrypt(t.getLeft(), RestClient.getEncryptionKey(), RestClient.getEncryptionVector()),
                    Encryption.decrypt(t.getMiddle(), RestClient.getEncryptionKey(), RestClient.getEncryptionVector()),
                    Encryption.decrypt(t.getRight(), RestClient.getEncryptionKey(), RestClient.getEncryptionVector()));
        } catch (IllegalBlockSizeException | BadPaddingException | IllegalArgumentException e) {
            throw new TamperingException();
        }
    }

    private static List<Triple<String, String, String>> checkIntegrity(List<Triple<String, String, String>> J,
                                          QueryMessage queryMessage) {
        TwinCondition cTwin = queryMessage.getTwinCondition();
        List<String> values  = cTwin.getValues();
        int markerCounter = queryMessage.getNumberOfMarkers();
        int replicationFactor = queryMessage.getReplicationFactor();
        ResultService.markerCounter = 0;
        Set<Pair<String, String>> M = Objects.requireNonNull(ResultService.generateMarkers(markerCounter,
                queryMessage.getMinNumberOfMarkers(),
                queryMessage.getMaxNumberOfMarkers(),
                queryMessage.getWorkers()));

        List<Triple<String, String, String>> result = new ArrayList<>(J);
        List<Triple<String, String, String>> T = new ArrayList<>();

        if (values != null && !values.isEmpty()) {
            for (Triple<String,String,String> t : J) {
                if (t.getLeft().startsWith("marker")) {
                    markerCounter--;
                    assert t.getMiddle().equals(t.getRight());
                    M.remove(Pair.of(Encryption.getPrefix(t.getMiddle(), "|"), t.getLeft()));
                    result.remove(t);
                } else if (values.contains(t.getLeft())) {
                    int tCounter = Collections.frequency(T, t);
                    if (tCounter == (replicationFactor - 1)) {
                        for (int i = 0; i < replicationFactor - 1; i++) {
                            T.remove(t);
                            result.remove(t);
                        }
                    } else {
                        T.add(t);
                    }
                }
            }
        } else {
            BigInteger invPTwin = new BigInteger(String.valueOf((int) Math.floor(1 / cTwin.getPTwin())));

            for (Triple<String,String,String> t : J) {
                if (t.getLeft().startsWith("marker")) {
                    markerCounter--;
                    assert t.getMiddle().equals(t.getRight());
                    M.remove(Pair.of(Encryption.getPrefix(t.getMiddle(), "|"), t.getLeft()));
                    result.remove(t);
                } else if ((Objects.requireNonNull(Encryption.hash(t.getLeft(), RestClient.getHashKey()))
                        .mod(invPTwin).equals(BigInteger.ZERO))) {
                    int tCounter = Collections.frequency(T, t);
                    if (tCounter == (replicationFactor - 1)) {
                        for (int i = 0; i < replicationFactor - 1; i++) {
                            T.remove(t);
                            result.remove(t);
                        }
                    } else {
                        T.add(t);
                    }
                }
            }
        }

        long endTime = System.nanoTime();

        if (markerCounter > 0 || !T.isEmpty()) {
            ResultService.remainingMarkers.addAll(M);
            ResultService.remainingTwins.addAll(T);
            ResultService.totalControlTuples = J.size() - result.size() + markerCounter + T.size();
            ResultService.finalByteSize = ResultService.initialByteSize - sizeOfTripleList(result) + sizeOfMarkers(M) + sizeOfTripleList(T);
            ResultService.elapsed = (endTime - ResultService.startTime) / 1000000;
            return null;
        }

        ResultService.finalByteSize = ResultService.initialByteSize - sizeOfTripleList(result);
        ResultService.elapsed = (endTime - ResultService.startTime) / 1000000;
        return result;
    }

    private static void insertDb(List<List<String>> rows) {
        try {
            PreparedStatement insertStatement = RestClient.connect.prepareStatement("INSERT INTO " +
                    RestClient.getClientTableName() + " VALUES (DEFAULT, ?,?,?,?,?,?)");

            int i = 0;
            RestClient.connect.setAutoCommit(false);
            for (List<String> row: rows) {
                insertStatement.setString(1, row.get(0)); // Lid
                insertStatement.setString(2, row.get(1)); // Lname
                insertStatement.setString(3, row.get(2)); // Lage
                insertStatement.setString(4, row.get(3)); // Rid
                insertStatement.setString(5, row.get(4)); // Rname
                insertStatement.setString(6, row.get(5)); // Rdisease
                insertStatement.addBatch();
                i++;
                if (i % ModelUtils.BATCH_SIZE == 0 || i == rows.size()) {
                    insertStatement.executeBatch();
                    RestClient.connect.commit();
                }
            }
        } catch (SQLException e) {
            Logger.err(ResultService.class, e);
        } finally {
            try {
                RestClient.connect.setAutoCommit(true);
            } catch (SQLException e) {
                Logger.err(ResultService.class, e);
            }
        }
    }

    private static String parseTid(String s) {
        if (s.contains("|")) {
            String complexTid = s.split("\\|")[0];
            if (complexTid.contains("-")) {
                return complexTid.split("-")[0];
            }
        }
        return null;
    }

    private static Set<Pair<String, String>> generateMarkers(int n, int nMin, int nMax, List<String> workers) {
        if (nMin < 0 || nMax < nMin || n < nMin) {
            Logger.err(ResultService.class, "Parameters' values are incorrect!");
            return null;
        }
        if (workers == null || workers.isEmpty()) {
            Logger.err(ResultService.class, "Workers' list is empty!");
            return null;
        }
        if (n == 0) {
            Logger.warn(ResultService.class, "The number of markers to generate is 0");
            return new HashSet<>();
        }
        int l = workers.size();
        if (l * nMax < n) {
            Logger.err(ResultService.class, "Cannot generate " + n + " markers with " + l + " workers and nMax = " + nMax);
            return null;
        }

        Set<Pair<String, String>> M = new HashSet<>();
        int spare = n - (nMin * l);
        Map<String, Integer> numMarkers = new HashMap<>();
        for (String worker : workers) {
            numMarkers.put(worker, 0);
        }

        do {
            String m = getNextMarker();
            String w = ComputationalServer.assignTupleToWorker(Objects.requireNonNull(
                    Encryption.encrypt(m, RestClient.getEncryptionKey(), RestClient.getEncryptionVector())), l);
            if ((numMarkers.get(w) < nMin) || ((numMarkers.get(w) < nMax && spare > 0))) {
                numMarkers.put(w, numMarkers.get(w) + 1);
                if (numMarkers.get(w) > nMin) {
                    spare--;
                }
                M.add(Pair.of((-1 * ResultService.markerCounter) + "_m", m));
            }
        } while (M.size() != n);

        Logger.info(ResultService.class, "Generated markers (" + M.size() + "): " + M, LogLevel.COMPLETE);
        return M;
    }

    private static String getNextMarker() {
        return "marker_" + ResultService.markerCounter++;
    }

    private static long sizeOfTripleList(List<Triple<String, String, String>> tupleList) {
        long totalSize = 0;
        for (var tuple: tupleList) {
            totalSize += tuple.getLeft().length();
            totalSize += tuple.getMiddle().length();
            totalSize += tuple.getRight().length();
        }
        return totalSize;
    }

    private static long sizeOfMarkers(Set<Pair<String, String>> tupleList) {
        long totalSize = 0;
        for (var tuple: tupleList) {
            totalSize += tuple.getLeft().length() * 2L;
            totalSize += tuple.getRight().length() * 3L;
            totalSize += 2;
        }
        return totalSize;
    }
}