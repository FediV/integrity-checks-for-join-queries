package client;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import utility.LogLevel;
import utility.Logger;
import utility.Network;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * The HttpServerThread class.
 * Server functionality for the client.
 * Default address: 127.0.0.1:1340
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public class HttpServerThread extends Thread {
    private static final String LOCAL_HOST = "localhost";
    private String ipAddr;
    private int port;
    private HttpServer server;
    private static String name;

    public HttpServerThread(String name, String ipAddr, int port) {
        HttpServerThread.setServerName(name);
        this.setIpAddr(ipAddr);
        this.setPort(port);
    }

    @Override
    public void run() {
        try {
            this.startServer();
        } catch (IOException e) {
            try {
                this.setIpAddr(HttpServerThread.LOCAL_HOST);
                this.startServer();
            } catch (IOException ex) {
                Logger.err(HttpServerThread.class,"Unable to create an HTTP server on " + this.getPort());
                ex.printStackTrace();
            }
        }
    }

    private void startServer() throws IOException {
        ResourceConfig config = new ResourceConfig().packages("client.services");
        this.server = GrizzlyHttpServerFactory.createHttpServer(URI.create(Network.createURL(this.getIpAddr(), this.getPort())), config);
        this.server.start();
        Logger.info(this, "Server " + HttpServerThread.getServerName() + " running!", LogLevel.REQUIRED);
        Logger.info(this, "Server running at: " + URI.create(Network.createURL(this.getIpAddr(), this.getPort())), LogLevel.REQUIRED);
    }

    public void stopServer() {
        Logger.info(this,"Stopping server...", LogLevel.REQUIRED);
        this.server.shutdown(100, TimeUnit.MILLISECONDS);
        Logger.info(this,"Server stopped", LogLevel.REQUIRED);
        System.exit(0);
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public void setPort(int port) {
        if (port >= 0 && port <= 65535) {
            this.port = port;
        } else {
            this.port = 1340;
            Logger.err(this, "Invalid port number: " + port + " | port set to default (1340)");
        }
    }

    public String getIpAddr() {
        return this.ipAddr;
    }

    public int getPort() {
        return this.port;
    }

    public static String getServerName() {
        return HttpServerThread.name;
    }

    public static void setServerName(String name) {
        HttpServerThread.name = name;
    }
}
