package utility;

/**
 * The LogLevel enumeration.
 * Define multiple levels for the logger:
 *  0 - ERROR
 *  1 - WARNING
 *  2 - REQUIRED
 *  3 - COMPLETE
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public enum LogLevel {
    ERROR(0),
    WARNING(1),
    REQUIRED(2),
    COMPLETE(3);

    private final int level;
    LogLevel(int logLevel) {
        this.level = logLevel;
    }

    public int getLevel() {
        return level;
    }
}
