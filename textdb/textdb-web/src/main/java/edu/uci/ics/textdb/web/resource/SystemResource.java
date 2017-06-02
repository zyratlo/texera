package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.TableMetadata;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Path("/resources")
@Produces(MediaType.APPLICATION_JSON)
public class SystemResource {
	@GET
	@Path("/metadata")
	public TextdbWebResponse getMetadata() throws Exception {
		List<TableMetadata> tableMetadata = RelationManager.getRelationManager().getMetaData();
		return new TextdbWebResponse(0, new ObjectMapper().writeValueAsString(tableMetadata));
	}

	@GET
	@Path("/dictionaries")
	/**
	 * Get the list of dictionaries
	 */
	public TextdbWebResponse getDictionaries() throws JsonProcessingException {
		RelationManager relationManager = RelationManager.getRelationManager();
		HashMap<String, String> dictionaries = relationManager.getDictionaries();

		return new TextdbWebResponse(0, new ObjectMapper().writeValueAsString(dictionaries));
	}

	@GET
	@Path("/dictionary")
	/**
	 * Get the content of dictionary
	 */
	public TextdbWebResponse getDictionary(@QueryParam("id") String id) throws IOException {
		RelationManager relationManager = RelationManager.getRelationManager();
		String dictionaryPath = relationManager.getDictionaryPath(id);

		if (dictionaryPath == null) {
			return new TextdbWebResponse(0, "No such dictionary found");
		}

		String content = readFromFile(dictionaryPath);

		return new TextdbWebResponse(0, content);
	}

	private String readFromFile(String dictionaryPath) throws IOException {
		String content = "";
		List<String> lines = Files.lines(Paths.get(dictionaryPath)).collect(Collectors.toList());
		for (String line : lines) {
			content = content.concat(line).concat(",");
		}

		// remove last ,
		if (content.charAt(content.length()-1) == ',') {
			content = content.substring(0, content.length()-1);
		}

		return content;
	}
}