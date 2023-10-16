package utility;

import org.apache.derby.iapi.services.io.FileUtil;

import java.io.File;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The ModelUtils class.
 * Manage DB connections and queries.
 * It uses Apache Derby (Java BD) through JDBC.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class ModelUtils {
    private static final String START_NET_SERVER_CMD = "cmd.exe /c C:\\Apache\\db-derby-10.16.1.1-bin\\bin\\startNetworkServer.bat";
    private static final String STOP_NET_SERVER_CMD = "cmd.exe /c C:\\Apache\\db-derby-10.16.1.1-bin\\bin\\stopNetworkServer.bat";

    public static final int BATCH_SIZE = 100;

    private ModelUtils() {}

    public static Connection startDBConnection(String dbUrl) {
        Runtime rt = Runtime.getRuntime();
        Connection connect = null;
        try {
            rt.exec(START_NET_SERVER_CMD);
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            Logger.info(ModelUtils.class, "Trying to connect to existing DB: " + dbUrl, LogLevel.COMPLETE);
            connect = DriverManager.getConnection(dbUrl);
        } catch (SQLException e) {
            Logger.info(ModelUtils.class, "DB not found, creating: " + dbUrl, LogLevel.COMPLETE);
            try {
                connect = DriverManager.getConnection(dbUrl + ";create=true");
            } catch (SQLException ex) {
                Logger.err(ModelUtils.class, ex);
                ModelUtils.closeDBConnection(null);
            }
        } catch (Exception e) {
            Logger.err(ModelUtils.class, e);
            ModelUtils.closeDBConnection(null);
        }
        return connect;
    }

    public static void modifyDB(String query, Connection connect) {
        try {
            if (connect == null) {
                throw new SQLException("DB connection is down!");
            }
            Statement statement = connect.createStatement();
            statement.execute(query);
        } catch (SQLException e) {
            Logger.err(ModelUtils.class, e);
        }
    }

    public static ResultSet queryDB(String query, Connection connect) {
        ResultSet result = null;
        try {
            if (connect == null) {
                throw new SQLException("DB connection is down!");
            }
            Statement statement = connect.createStatement();
            result = statement.executeQuery(query);
        } catch (SQLException e) {
            Logger.err(ModelUtils.class, e);
        }
        return result;
    }

    // DB soft reset
    public static void clearTables(boolean deleteTables, List<String> tableNames, Connection connect) {
        if (tableNames != null) {
            tableNames = tableNames.stream().map(String::toLowerCase).collect(Collectors.toList());
        }
        try {
            DatabaseMetaData metaData = connect.getMetaData();
            ResultSet resultSet = metaData.getTables(null, null, null, new String[]{"TABLE"});
            if (deleteTables) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString(3);
                    if (tableNames != null && tableNames.contains(tableName.toLowerCase())) {
                        Logger.warn(ModelUtils.class, "Deleting table: " + tableName);
                        ModelUtils.modifyDB("DROP TABLE " + tableName, connect);
                    }
                }
            } else {
                while (resultSet.next()) {
                    String tableName = resultSet.getString(3);
                    if (tableNames != null && tableNames.contains(tableName.toLowerCase())) {
                        Logger.warn(ModelUtils.class, "Deleting content from table: " + tableName);
                        ModelUtils.modifyDB("DELETE FROM " + tableName, connect);
                    }
                }
            }
        } catch(SQLException e) {
            Logger.err(ModelUtils.class, e);
        }
    }

    // DB hard reset
    public static boolean deleteDB(String dbPath) {
        File dbDir = new File(new File("").getAbsolutePath() + "/" + dbPath);
        if (dbDir.isDirectory()) {
            Logger.warn(ModelUtils.class, "DB: " + dbPath + " has been deleted!");
            return FileUtil.removeDirectory(dbDir);
        }
        return false;
    }

    public static void closeDBConnection(Connection connect, ResultSet resultSet, Statement statement) {
        Runtime rt = Runtime.getRuntime();
        try {
            Logger.info(ModelUtils.class, "Closing DB connection...", LogLevel.COMPLETE);
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connect != null) {
                connect.close();
            }
            rt.exec(STOP_NET_SERVER_CMD);
        } catch (Exception e) {
            Logger.err(ModelUtils.class, e);
        }
    }

    public static void closeDBConnection(Connection connect) {
        Runtime rt = Runtime.getRuntime();
        try {
            Logger.info(ModelUtils.class, "Closing DB connection...", LogLevel.COMPLETE);
            if (connect != null) {
                connect.close();
            }
            rt.exec(STOP_NET_SERVER_CMD);
        } catch (Exception e) {
            Logger.err(ModelUtils.class, e);
        }
    }

}
