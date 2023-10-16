package server.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import communication.DistributedJoinQueryMessage;
import communication.JoinQueryMessage;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import server.ComputationalServer;
import utility.CustomJsonParser;
import utility.LogLevel;
import utility.Logger;

/**
 * The ComputationService class.
 * Exposed by: ComputationalServer
 * Endpoints:
 *  /join
 *  /distributed-join
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@Path("")
public class ComputationService {

    @Path("join")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response process(String message) {
        JoinQueryMessage joinMessage;
        try {
            joinMessage = CustomJsonParser.deserializeObject(message, JoinQueryMessage.class);
            int numOfRelations = ComputationalServer.addRelation(joinMessage.getSender(), joinMessage);
            Logger.info(this, joinMessage.toString(), LogLevel.REQUIRED);
            if (numOfRelations == 2) {
                ComputationalServer.join();
            }
        } catch (JsonProcessingException e) {
            Logger.err(this, e);
            return Response.serverError().build();
        }

        return Response.ok().build();
    }

    @Path("distributed-join")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response distributedJoin(String message) {
        DistributedJoinQueryMessage distributedJoinMessage;
        try {
            distributedJoinMessage = CustomJsonParser.deserializeObject(message, DistributedJoinQueryMessage.class);
            JoinQueryMessage joinMessage = distributedJoinMessage.getJoinQueryMessage();
            int numOfRelations = ComputationalServer.addDistributedMessage(joinMessage.getSender(), distributedJoinMessage);
            Logger.info(this, distributedJoinMessage.toString(), LogLevel.REQUIRED);
            if (numOfRelations == 2) {
                ComputationalServer.distributedJoin();
            }
        } catch (JsonProcessingException e) {
            Logger.err(this, e);
            return Response.serverError().build();
        }

        return Response.ok().build();
    }
}
