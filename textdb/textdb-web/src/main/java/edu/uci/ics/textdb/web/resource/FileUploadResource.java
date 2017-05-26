package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.TextdbWebException;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.*;

@Path("/upload")
@Consumes(MediaType.MULTIPART_FORM_DATA)
@Produces(MediaType.APPLICATION_JSON)
public class FileUploadResource {
	@POST
	@Path("/dictionary")
	public TextdbWebResponse uploadDictionaryFile(
					@FormDataParam("file") InputStream uploadedInputStream,
					@FormDataParam("file") FormDataContentDisposition fileDetail) throws Exception {

		StringBuilder dictionary = new StringBuilder();

		String line = "";
		try (BufferedReader br = new BufferedReader(new InputStreamReader(uploadedInputStream))) {
			while ((line = br.readLine()) != null) {
				dictionary.append(line);
			}
		}

		System.out.println(dictionary.toString());

		/*if (uploadedInputStream == null || fileDetail == null) {
			return new TextdbWebResponse(0, "Make sure you provide dictionary");
		}
		String uploadedFileLocation = "dictionary/" + fileDetail.getFileName();

		// save it
		writeToFile(uploadedInputStream, uploadedFileLocation);*/

		return new TextdbWebResponse(0, "Dictionary is uploaded");
	}

	// save uploaded file to a new location
	private void writeToFile(InputStream uploadedInputStream, String uploadedFileLocation) {
		try {
			OutputStream out = new FileOutputStream(new File(uploadedFileLocation));
			int read = 0;
			byte[] bytes = new byte[1024];

			while ((read = uploadedInputStream.read(bytes)) != -1) {
				out.write(bytes, 0, read);
			}
			out.flush();
			out.close();
		} catch (IOException e) {
			throw new TextdbWebException("Error occurred while uploading dictionary");
		}
	}
}