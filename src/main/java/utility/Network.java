package utility;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * The Network class.
 * Manage GET and POST HTTP requests.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public final class Network {
    private Network() {}

    public static String createURL(String serverAddr, int port, String path) {
        if (path != null && !path.isBlank()) {
            return "http://" + serverAddr + ":" + port + "/" + path;
        }
        return "http://" + serverAddr + ":" + port + "/";
    }

    public static String createURL(String serverAddr, int port) {
        return "http://" + serverAddr + ":" + port + "/";
    }

    public static <T> Response post(Client sender, String targetUrl, T postObj){
        Logger.info(Network.class, "POST request to: " + targetUrl, LogLevel.REQUIRED);
        return sender.target(targetUrl)
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.entity(postObj, MediaType.APPLICATION_JSON));
    }

    public static Response setParams(Client sender, String targetUrl, long seed, String joinAttribute,
                                     String joinAttributeFilePath) {
        Logger.info(Network.class, "GET request with params. to: " + targetUrl, LogLevel.REQUIRED);
        return sender.target(targetUrl)
                .queryParam("seed", seed)
                .queryParam("joinAttribute", joinAttribute)
                .queryParam("joinAttributeFilePath", joinAttributeFilePath)
                .request().get();
    }

    public static Response get(Client sender, String targetUrl){
        Logger.info(Network.class, "GET request to: " + targetUrl, LogLevel.REQUIRED);
        return sender.target(targetUrl).request().get();
    }
}
