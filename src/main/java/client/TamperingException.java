package client;

/**
 * The TamperingException class.
 * Custom exception for the integrity checks.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class TamperingException extends Exception {
    public TamperingException(String msg) {
        super("DETECTED TAMPERING: " + msg);
    }
    public TamperingException() {
        super("DETECTED TAMPERING!");
    }
}
