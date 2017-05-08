package edu.uci.ics.textdb.web.resource;

import java.nio.file.Files;
import java.nio.file.Paths;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.api.utils.Utils;

@Path("/download")
@Produces("application/vnd.ms-excel")
@Consumes(MediaType.APPLICATION_JSON)
public class DownloadFileResource {
    
    @GET
    @Path("/{resultTimeStamp}")
    public Response downloadExcelFile(@PathParam("resultTimeStamp") String resultTimeStamp) {

        String excelDirectory = Utils.getResourcePath("/index/excel", DataConstants.TextdbProject.TEXTDB_EXP);
        String resultFileName = resultTimeStamp + ".xlsx";
        String excelFilePath = Paths.get(excelDirectory, resultFileName).toString();
        
        if (Files.exists(Paths.get(excelFilePath))) {
            return Response.noContent().build();
        } else {
            ResponseBuilder response = Response.ok(Paths.get(excelFilePath).toFile());
            response.header("Content-Disposition", "attachment; filename=" + resultFileName);
            return response.build();
        }
    }

}
