package edu.uci.ics.textdb.sandbox.team2luceneexample;

/**
 * Created by shiladityasen on 06/04/16.
 */

import org.apache.lucene.*;
import edu.uci.ics.textdb.sandbox.team2luceneexample.Data;

public class Pokedexer
{
    public static void main(String[] args)
    {
        for(int i=0; i<Data.POKEMONS.length; i++)
            System.out.println(Data.POKEMONS[i].name);
    }
}

