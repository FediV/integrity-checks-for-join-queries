package client.services;

import client.RestClient;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import utility.LogLevel;
import utility.Logger;
import utility.Network;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The ConfigurationService class.
 * Exposed by: HttpServerThread
 * Endpoints:
 *  /config/nmax-s
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@Path("config")
public class ConfigurationService {
	private static final List<Integer> receivedNmax = new ArrayList<>();

	@Path("nmax-s")
	@POST
	@Consumes({MediaType.APPLICATION_JSON})
	public Response generateSalts(int nmaxStorageServer) {
		synchronized (receivedNmax) {
			receivedNmax.add(nmaxStorageServer);
			if (receivedNmax.size() == 2) {
				int s;
				if (!receivedNmax.contains(1)) { // Join is not 1:n (not supported!)
					s = -1;
				} else {
					int nmax = Collections.max(receivedNmax);
					s =  (int) Math.ceil(Math.sqrt(nmax));
				}
				Logger.info(this, "Received nmax(s): " + receivedNmax + " | Calculated number of salts: " + s, LogLevel.COMPLETE);
				receivedNmax.clear();
				Response response;
				Network.post(
						RestClient.getClient(),
						Network.createURL(
								RestClient.getStorageServerAddrL(),
								RestClient.getStorageServerPortL(),
								RestClient.getNumberOfSaltsPath()), s);
				response = Network.post(
						RestClient.getClient(),
						Network.createURL(
								RestClient.getStorageServerAddrR(),
								RestClient.getStorageServerPortR(),
								RestClient.getNumberOfSaltsPath()), s);
				return response;
			}
		}
		return Response.ok().build();
	}
}
