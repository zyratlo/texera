package edu.uci.ics.textdb.sandbox.team2luceneexample;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by kishorenarendran on 06/04/16.
 */

public class GetPokemonInfo {

    //Intiializing base URL of the Poke API and User agent
    private static final String API_URL = "http://pokeapi.co/api/v2/pokemon/";
    private final String USER_AGENT = "Mozilla/5.0";

    //Data Members
    public int numberOfPokemon;
    public Pokemon[] pokemonInfo;

    /**
     * Parameterized constructor for the class which assigns number of pokemon
     * and initializes the number of pokemonInfo objects
     * @param numberOfPokemon - Parameter to initialize an object of GetPokemonInfo
     */
    public GetPokemonInfo(int numberOfPokemon) {
        this.numberOfPokemon = numberOfPokemon;
        this.pokemonInfo = new Pokemon[numberOfPokemon];
    }

    /**
     * Getter method for the number of pokemon data member
     * @return - numberOfPokemon data member
     */
    public int getNumberOfPokemon() {
        return numberOfPokemon;
    }

    /**
     * Setter method for the number of pokemon data member
     * @param numberOfPokemon - Parameter to set the numberOfPokemon data member
     */
    public void setNumberOfPokemon(int numberOfPokemon) {
        this.numberOfPokemon = numberOfPokemon;
    }

    /**
     * Getter method
     * @return - pokemonInfo data member
     */
    public Pokemon[] getPokemonInfo() {
        return pokemonInfo;
    }

    /**
     * This function sends a GET request to the PokeAPI to get
     * information about a Pokemon of a specific ID
     * @param id - Pokemon ID, for which information is request
     * @return - String response from GET request
     * @throws IOException
     */
    private String sendPokemonInfoGetRequest(int id) throws IOException {

        //Performing GET request to PokeAPI
        String requestUrl = API_URL.concat(Integer.toString(id));
        URL obj = new URL(requestUrl);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        StringBuffer response = new StringBuffer();
        con.setRequestProperty("User-Agent", USER_AGENT);

        //Getting response code from the request
        int responseCode = con.getResponseCode();

        //Checking for success response code 200
        if(responseCode == 200) {
            //Forming the response string
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            return response.toString();
        }
        else {
            return null;
        }
    }

    /**
     * Getting an array of pokemon info response strings on pinging
     * the Poke API
     * @return - Array of string HTTP GET responses for all the pokemon IDs
     * @throws IOException
     */
    private String[] getPokemonInfoStrings() throws IOException {
        String[] allPokemonInfo = new String[numberOfPokemon];
        for(int i = 1; i <= numberOfPokemon; i++) {
            String pokemonInfoStr = sendPokemonInfoGetRequest(i);
            if(pokemonInfoStr == null) {
                allPokemonInfo[i-1] = "";
            }
            else {
                allPokemonInfo[i-1] = pokemonInfoStr;
            }

        }
        return allPokemonInfo;
    }

    /**
     * Parses the pokemon info response strings as JSON objects,
     * and then aggregating an array of Pokemon objects as the pokemonInfo data member
     * with necessary information as described by the data members of the Pokemon class
     * @throws IOException
     */
    public void aggregatePokemonInfo() throws IOException {
        String[] pokemonInfoStrs = getPokemonInfoStrings();
        for(int i = 1; i <= numberOfPokemon; i++) {
            //Converting the info string to JSON object
            String pokemonInfoStr = pokemonInfoStrs[i-1];

            //Skipping JSON parsing for empty/malformed response strings
            if(pokemonInfoStr.equals("")) {
                pokemonInfo[i-1] = null;
                continue;
            }

            JSONObject pokemonInfoJsonObj = new JSONObject(pokemonInfoStr);

            //Parsing the JSON object for necessary information
            String pokemonName = pokemonInfoJsonObj.getString("name");
            int pokemonId = pokemonInfoJsonObj.getInt("id");
            String pokemonMovesStr = "";
            JSONArray pokemonMoves = pokemonInfoJsonObj.getJSONArray("moves");
            for(int j = 0; j <pokemonMoves.length(); j++) {
                String pokemonMove = pokemonMoves.getJSONObject(j).getJSONObject("move").getString("name");
                pokemonMovesStr = pokemonMovesStr.concat(pokemonMove).concat(", ");
            }
            pokemonMovesStr = pokemonMovesStr.substring(0, pokemonMovesStr.length()-2);

            String pokemonTypesStr = "";
            JSONArray pokemonTypes = pokemonInfoJsonObj.getJSONArray("types");
            for(int j = 0; j <pokemonTypes.length(); j++) {
                String pokemonType = pokemonTypes.getJSONObject(j).getJSONObject("type").getString("name");
                pokemonTypesStr = pokemonTypesStr.concat(pokemonType).concat(", ");
            }
            pokemonTypesStr = pokemonTypesStr.substring(0, pokemonTypesStr.length()-2);

            //Creating a Pokemon object after parsing the JSON
            Pokemon pokemon = pokemonInfo[i-1];
            pokemon = new Pokemon(pokemonId, pokemonName, pokemonMovesStr, pokemonTypesStr);
            pokemonInfo[i-1] = pokemon;
        }
    }
}
