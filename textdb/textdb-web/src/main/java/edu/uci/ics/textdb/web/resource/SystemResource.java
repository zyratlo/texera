package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.exp.resources.dictionary.DictionaryManager;
import edu.uci.ics.textdb.exp.resources.dictionary.DictionaryManagerConstants;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.storage.TableMetadata;
import edu.uci.ics.textdb.web.TextdbWebException;
import edu.uci.ics.textdb.web.response.TextdbWebResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    /**
     * Get the list of dictionaries
     */
	@GET
	@Path("/dictionaries")
	public TextdbWebResponse getDictionaries() throws JsonProcessingException {
		DictionaryManager dictionaryManager = DictionaryManager.getInstance();
		List<String> dictionaries = dictionaryManager.getDictionaries();

		return new TextdbWebResponse(0, new ObjectMapper().writeValueAsString(dictionaries));
	}

	 /**
     * Get the content of dictionary
     */
	@GET
	@Path("/dictionary")
	public TextdbWebResponse getDictionary(@QueryParam("name") String name) throws IOException {
        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
        List<String> dictionaries = dictionaryManager.getDictionaries();
        
        if (! dictionaries.contains(name)) {
            throw new TextdbWebException("Dictionary " + name + " does not exist");
        }

		String content = readFromFile(Paths.get(DictionaryManagerConstants.DICTIONARY_DIR, name));

		return new TextdbWebResponse(0, content);
	}

	private String readFromFile(java.nio.file.Path dictionaryPath) throws IOException {
		return Files.lines(dictionaryPath).collect(Collectors.joining(","));

	}
}