package edu.uci.ics.textdb.web.resource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import edu.uci.ics.textdb.api.constants.DataConstants;
import edu.uci.ics.textdb.api.utils.Utils;

@Path("/download")
@Produces("application/vnd.ms-excel")
@Consumes(MediaType.APPLICATION_JSON)
public class DownloadFileResource {
    
    @GET
    @Path("/{resultTimeStamp}")
    public Response downloadExcelFile(@PathParam("resultTimeStamp") String resultTimeStamp) {
        System.out.println("resultTimeStamp is " + resultTimeStamp);
        
        String excelDirectory = Utils.getResourcePath("/index/excel", DataConstants.TextdbProject.TEXTDB_EXP);
        String resultFileName = resultTimeStamp + ".xlsx";
        String excelFilePath = Paths.get(excelDirectory, resultFileName).toString();
        
        if (Files.notExists(Paths.get(excelFilePath))) {
            System.out.println(excelFilePath + " file does not found");
            return Response.status(Status.NOT_FOUND).build();
        } else {
            System.out.println("reading " + resultFileName + " and sends out");
            StreamingOutput fileStream = new StreamingOutput() {
                @Override
                public void write(OutputStream output) throws IOException, WebApplicationException {
                    byte[] data = Files.readAllBytes(Paths.get(excelFilePath));
                    output.write(data);
                    output.flush();
                }
            };
            return Response.ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                    .header("content-disposition", "attachment; filename=" + resultFileName)
                    .build();
        }
    }

}
