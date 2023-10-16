package server.services;

import communication.DataConfigFile;
import model.DataGenerator;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import server.StorageServer;
import utility.CustomJsonParser;
import utility.LogLevel;
import utility.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The DataGenerationService class.
 * Exposed by: StorageServer
 * Endpoints:
 *  /send-params
 *  /send-config
 *  /generate
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
@Path("data")
public class DataGenerationService {
    private final static String DATA_CONFIG_FILE_PATH_SUFFIX = "_data.json";
    private final static List<String> dataGenerationFiles = new ArrayList<>(2);
    private static DataGenerator generator;

    @Path("send-params")
    @GET
    public Response sendParams(@QueryParam("seed") Long seed, @QueryParam("joinAttribute") String joinAttribute,
                               @QueryParam("joinAttributeFilePath") String joinAttributeFilePath) {
        DataGenerator.setSeed(seed);
        StorageServer.setJoinAttribute(joinAttribute);
        DataGenerator.setJoinAttributeFilePath(joinAttributeFilePath);
        return Response.ok().build();
    }

    @Path("send-config")
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response sendConfigFile(DataConfigFile dataConfigFile) {
        CustomJsonParser<DataConfigFile> jsonParser = new CustomJsonParser<>(dataConfigFile.getDbName() +
                DATA_CONFIG_FILE_PATH_SUFFIX, DataConfigFile.class);
        DataConfigFile oldDataConfigFile = jsonParser.readJsonFile();
        try {
            if (oldDataConfigFile == null || !oldDataConfigFile.equals(dataConfigFile)) {
                jsonParser.writeJsonFile(dataConfigFile);
                if (dataConfigFile.getSourceFilePath() != null) {
                    dataGenerationFiles.add(dataConfigFile.getSourceFilePath());
                }
                DataGenerationService.generator = new DataGenerator(dataConfigFile);
            } else {
                if (dataConfigFile.getSourceFilePath() != null) {
                    dataGenerationFiles.add(dataConfigFile.getSourceFilePath());
                }
                DataGenerationService.generator = new DataGenerator(oldDataConfigFile);
                Logger.info(this, "New config file is equal to the one already stored...", LogLevel.COMPLETE);
            }
        } catch (IOException e) {
            Logger.err(this, e);
            return Response.serverError().build();
        }
        return Response.ok().build();
    }

    @Path("generate")
    @GET
    public Response generateData() {
        synchronized (DataGenerationService.dataGenerationFiles) {
            if (DataGenerationService.generator != null && StorageServer.getJoinAttribute() != null) {
                DataGenerationService.generator.generate(DataGenerationService.dataGenerationFiles);
                Logger.info(this, "Join attr.: " + StorageServer.getJoinAttribute() +
                                " | Seed: " + DataGenerator.getSeed() +
                                " | Source files: " + DataGenerationService.dataGenerationFiles,
                        LogLevel.COMPLETE);
                DataGenerationService.dataGenerationFiles.clear();
                return Response.ok().build();
            }
            DataGenerationService.dataGenerationFiles.clear();
        }
        return Response.serverError().build();
    }
}
