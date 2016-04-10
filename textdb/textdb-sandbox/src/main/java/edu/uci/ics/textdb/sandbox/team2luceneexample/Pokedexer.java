package edu.uci.ics.textdb.sandbox.team2luceneexample;

/**
 * Created by shiladityasen on 06/04/16.
 */

import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;

//import edu.uci.ics.textdb.sandbox.team2luceneexample.LuceneConstants;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;
import java.util.Arrays;

import java.io.IOException;
import java.util.ArrayList;

import static com.sun.tools.doclint.Entity.ge;

public class Pokedexer {

    private IndexWriter ix_writer;

    public Pokedexer(String indexDirectoryPath) throws IOException {
        Directory direc = FSDirectory.open(Paths.get(indexDirectoryPath));
        IndexWriterConfig ixWriteConf = new IndexWriterConfig(new StandardAnalyzer());
        ix_writer = new IndexWriter(direc, ixWriteConf);
    }


    private Document getDocument(Pokemon pokemon) throws IOException {
        Document doc = new Document();

        //index pokemon info
        Field id = new IntField(LuceneConstants.ID_FIELD, pokemon.getId(), Field.Store.YES);
        Field name = new StringField(LuceneConstants.NAME_FIELD, pokemon.getName(), Field.Store.YES);

//        ArrayList<String> all_types = new ArrayList<String>(Arrays.asList(pokemon.getTypes().split("\\s*,\\s*")));
//        ArrayList<Field> types = new ArrayList();
//        for (int i = 0; i < types.size(); i++)
//            types.add(new StringField(LuceneConstants.TYPES_FIELD, all_types.get(i), Field.Store.YES));
        Field types = new TextField(LuceneConstants.TYPES_FIELD, pokemon.getTypes(), Field.Store.YES);

//        ArrayList<String> all_moves = new ArrayList(Arrays.asList(pokemon.getMoves().split("\\s*,\\s*")));
//        ArrayList<Field> moves = new ArrayList();
//        for (int i = 0; i < moves.size(); i++)
//            moves.add(new StringField(LuceneConstants.MOVES_FIELD, all_moves.get(i), Field.Store.NO));
        Field moves = new TextField(LuceneConstants.MOVES_FIELD, pokemon.getMoves(), Field.Store.NO);

        //add fields to doc
        doc.add(id);
        doc.add(name);
//        for (int i = 0; i < types.size(); i++)
//            doc.add(types.get(i));
//        for (int i = 0; i < moves.size(); i++)
//            doc.add(moves.get(i));
        doc.add(types);
        doc.add(moves);

        return doc;
    }


    public void addPokemon(Pokemon pokemon) throws IOException {
        Document doc = getDocument(pokemon);
        ix_writer.addDocument(doc);
    }

    public void addPokemon(int numberOfPokemon) throws IOException{
        GetPokemonInfo getPokemonInfo = new GetPokemonInfo(numberOfPokemon);
        getPokemonInfo.aggregatePokemonInfo();
        Pokemon[] pokemons = getPokemonInfo.getPokemonInfo();
        for(Pokemon pokemon: pokemons) {
            Document document = getDocument(pokemon);
            ix_writer.addDocument(document);
        }
    }

    public void closeIndexWriter() throws IOException {
        if (ix_writer != null) {
            ix_writer.close();
        }
    }

    public static void main(String args[]) throws IOException {
        Pokedexer pokedexer = new Pokedexer(LuceneConstants.INDEX);
        pokedexer.addPokemon(10);
        pokedexer.closeIndexWriter();
    }
}