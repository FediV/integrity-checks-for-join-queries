package server.services;

import communication.QueryMessage;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import model.QueryHandler;
import utility.Logger;

import java.util.List;

/**
 * The QueryHandlingService class.
 * Exposed by: StorageServer
 * Endpoints:
 *  /process
 *  /semi-join
 *  /number-of-salts
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@Path("query")
public class QueryHandlingService {
    private static QueryHandler queryHandler;

    @Path("process")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response process(QueryMessage queryMessage) {
        QueryHandlingService.queryHandler = new QueryHandler(queryMessage);
        if (queryMessage.getUseOccurrences()) {
            QueryHandlingService.queryHandler.setupJoinWithOccurrences();
        } else {
            QueryHandlingService.queryHandler.setupRegularJoin();
        }

        return Response.ok().build();
    }

    @Path("semi-join")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response semiJoin(List<String> tids) {
        if (QueryHandlingService.queryHandler != null) {
            try {
                return Response.ok(QueryHandlingService.queryHandler.semiJoin(tids)).build();
            } catch (Exception e) {
                Logger.err(this, e);
                return Response.serverError().build();
            }
        }
        return Response.serverError().build();
    }

    @Path("number-of-salts")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response setNumberOfSalts(int s) {
        if (QueryHandlingService.queryHandler != null) {
            QueryHandlingService.queryHandler.setNumberOfSalts(s);
        }
        return Response.serverError().build();
    }
}
