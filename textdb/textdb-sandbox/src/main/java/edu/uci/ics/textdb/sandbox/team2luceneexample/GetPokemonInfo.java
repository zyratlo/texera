package edu.uci.ics.textdb.sandbox.team2luceneexample;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by kishorenarendran on 06/04/16.
 */

public class GetPokemonInfo {
    private static String apiUrl = "http://pokeapi.co/api/v2/pokemon/";
    private final String USER_AGENT = "Mozilla/5.0";

    public int numberOfPokemon;

    public GetPokemonInfo(int numberOfPokemon) {
        this.numberOfPokemon = numberOfPokemon;
    }

    public int getNumberOfPokemon() {
        return numberOfPokemon;
    }

    public void setNumberOfPokemon(int numberOfPokemon) {
        this.numberOfPokemon = numberOfPokemon;
    }

    private String sendPokemonInfoGetRequest(int id) throws Exception {

        //Performing GET request to PokeAPI
        String requestUrl = apiUrl.concat(Integer.toString(id));
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

    private String[] aggregatePokemonInfo() throws Exception {
        String[] allPokemonInfo = new String[numberOfPokemon];
        for(int i = 1; i <= numberOfPokemon; i++) {
            allPokemonInfo[i-1] = sendPokemonInfoGetRequest(i);
        }
        return allPokemonInfo;
    }

    private Pokemon[] getPokemonInfoObjects() throws Exception{
        String[] pokemonInfoStrs = aggregatePokemonInfo();
        Pokemon[] pokemonInfos = new Pokemon[pokemonInfoStrs.length];
        for(int i = 1; i <= numberOfPokemon; i++) {
            //Converting the info string to JSON object
            String pokemonInfoStr = pokemonInfoStrs[i-1];
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
            Pokemon pokemonInfo = pokemonInfos[i-1];
            pokemonInfo = new Pokemon(pokemonId, pokemonName, pokemonMovesStr, pokemonTypesStr);
            pokemonInfos[i-1] = pokemonInfo;
        }
        return pokemonInfos;
    }
}
