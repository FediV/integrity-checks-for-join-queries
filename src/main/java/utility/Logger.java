package utility;

/**
 * The Logger class.
 * Custom multiple level logger.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class Logger {
    public static LogLevel level = LogLevel.COMPLETE;

    private Logger() {}

    public static void info(Class<?> cls, String msg, LogLevel logLevel) {
        if (logLevel.getLevel() <= Logger.level.getLevel()) {
            System.out.println("[" + cls.getSimpleName() + "] " + msg);
        }
    }

    public static <T> void info(T obj, String msg, LogLevel logLevel) {
        if (logLevel.getLevel() <= Logger.level.getLevel()) {
            System.out.println("[" + obj.getClass().getSimpleName() + "] " + msg);
        }
    }

    public static void info(Class<?> cls, String msg, TextColor color) {
        System.out.println(color.colorText("[" + cls.getSimpleName() + "] " + msg));
    }

    public static <T> void info(T obj, String msg, TextColor color) {
        System.out.println(color.colorText("[" + obj.getClass().getSimpleName() + "] " + msg));
    }

    public static void err(Class<?> cls, String msg) {
        System.err.println("[" + cls.getSimpleName() + "] " + msg);
    }

    public static <T> void err(T obj, String msg) {
        System.err.println("[" + obj.getClass().getSimpleName() + "] " + msg);
    }

    public static void err(Class<?> cls, Exception e, boolean printStackTrace) {
        System.err.println("[" + cls.getSimpleName() + "] " + e.getMessage());
        if (printStackTrace) {
            e.printStackTrace();
        }
    }

    public static <T> void err(T obj, Exception e, boolean printStackTrace) {
        System.err.println("[" + obj.getClass().getSimpleName() + "] " + e.getMessage());
        if (printStackTrace) {
            e.printStackTrace();
        }
    }

    public static void err(Class<?> cls, Exception e) {
        Logger.err(cls, e, true);
    }

    public static <T> void err(T obj, Exception e) {
        Logger.err(obj, e, true);
    }

    public static void warn(Class<?> cls, String msg) {
        if (Logger.level.getLevel() >= LogLevel.WARNING.getLevel()) {
            System.out.println("[" + cls.getSimpleName() + "] WARNING: " + msg);
        }
    }

    public static <T> void warn(T obj, String msg) {
        if (Logger.level.getLevel() >= LogLevel.WARNING.getLevel()) {
            System.out.println("[" + obj.getClass().getSimpleName() + "] WARNING: " + msg);
        }
    }
}
