package utility;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Statistics class.
 * Compute size for directories and tables.
 * It uses Apache Derby (Java BD) through JDBC.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class Statistics {
    public static final String[] MEM_UNITS = new String[] { "B", "KB", "MB", "GB", "TB" };

    private Statistics() {}

    public static long size(Path path) {
        final AtomicLong size = new AtomicLong(0);

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
           Logger.err(Statistics.class, e);
        }

        return size.get();
    }

    public static String getFolderSize(Path path) {
        return Statistics.getHumanReadableSize(Statistics.size(path));
    }

    public static String getHumanReadableSize(long size) {
        int unitIndex = (int) (Math.log10(size) / 3);
        double unitValue = 1 << (unitIndex * 10);

        return new DecimalFormat("#,##0.00").format(size / unitValue) + " " + Statistics.MEM_UNITS[unitIndex];
    }

    public static long tableSize(String dbUrl, Connection connect) {
        if (connect == null) {
            connect = ModelUtils.startDBConnection(dbUrl);
        }
        long size = -1L;
        ResultSet result = ModelUtils.queryDB("SELECT SUM(T2.NUMALLOCATEDPAGES * T2.PAGESIZE) AS size" +
                " FROM SYS.SYSTABLES systabs, SYS.SYSSCHEMAS sysschemas, TABLE (SYSCS_DIAG.SPACE_TABLE()) AS T2" +
                " WHERE systabs.tabletype = 'T' AND sysschemas.schemaid = systabs.schemaid AND systabs.tableid = T2.tableid",
                connect);
        try {
            if (result.next()) {
                size = result.getLong("size");
            }
            result.close();
        } catch (SQLException e) {
            Logger.err(Statistics.class, e);
        }
        return size;
    }

    public static String getTableSize(String dbUrl, Connection connect) {
        return Statistics.getHumanReadableSize(Statistics.tableSize(dbUrl, connect));
    }

    public static int getTableCount(String dbUrl, String tableName, Connection connect) {
        if (connect == null) {
            connect = ModelUtils.startDBConnection(dbUrl);
        }
        int count = 0;
        ResultSet result = ModelUtils.queryDB("SELECT COUNT(*) FROM " + tableName, connect);
        try {
            if (result.next()) {
                count = result.getInt(1);
            }
            result.close();
        } catch (SQLException e) {
            Logger.err(Statistics.class, e);
        }
        return count;
    }
}
