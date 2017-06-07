package edu.uci.ics.textdb.web.resource;

import edu.uci.ics.textdb.exp.resources.dictionary.DictionaryManager;
import edu.uci.ics.textdb.exp.resources.dictionary.DictionaryManagerConstants;
import edu.uci.ics.textdb.web.TextdbWebException;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import java.io.*;
import java.nio.file.*;

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
		} catch (IOException e) {
			e.printStackTrace();
			throw new TextdbWebException("Error occurred whlie uploading dictionary");
		}
		
		String fileName = fileDetail.getFileName();

		// save it as a file
		writeToFile(dictionary.toString(), fileName);

		// add dictionary to the table
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        dictionaryManager.addDictionary(fileName);

		return new TextdbWebResponse(0, "Dictionary is uploaded");
	}

	/**
	 * Write uploaded file at the given location (if the file exists, remove it and write a new one.)
	 *
	 * @param content
	 * @param fileUploadDirectory
	 * @param fileName
	 */
	private void writeToFile(String content, String fileName) {
		try {
			java.nio.file.Path filePath = Paths.get(DictionaryManagerConstants.DICTIONARY_DIR, fileName);

			if (! Files.exists(Paths.get(DictionaryManagerConstants.DICTIONARY_DIR))) {
		         Files.createDirectories(Paths.get(DictionaryManagerConstants.DICTIONARY_DIR));
			}
			Files.deleteIfExists(filePath);
			Files.createFile(filePath);
			Files.write(filePath, content.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
			throw new TextdbWebException("Error occurred whlie uploading dictionary");
		}
	}
}