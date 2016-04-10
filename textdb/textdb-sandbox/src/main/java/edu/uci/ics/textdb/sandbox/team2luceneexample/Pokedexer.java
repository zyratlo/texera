package edu.uci.ics.textdb.sandbox.team2luceneexample;

/**
 * Created by shiladityasen on 06/04/16.
 */

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;

import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;

import java.io.IOException;

public class Pokedexer {

    private IndexWriter indexWriter;
    private int numberOfPokemon;

    public Pokedexer(String indexDirectoryPath, int numberOfPokemon) throws IOException {
        Directory direc = FSDirectory.open(Paths.get(indexDirectoryPath));
        IndexWriterConfig ixWriteConf = new IndexWriterConfig(new StandardAnalyzer());
        indexWriter = new IndexWriter(direc, ixWriteConf);
        indexWriter.deleteAll();
        this.numberOfPokemon = numberOfPokemon;
    }

    public int getNumberOfPokemon() {
        return numberOfPokemon;
    }

    public void setNumberOfPokemon(int numberOfPokemon) {
        this.numberOfPokemon = numberOfPokemon;
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
        indexWriter.addDocument(doc);
    }

    public void addPokemon() throws IOException{
        GetPokemonInfo getPokemonInfo = new GetPokemonInfo(numberOfPokemon);
        getPokemonInfo.aggregatePokemonInfo();
        Pokemon[] pokemons = getPokemonInfo.getPokemonInfo();
        for(Pokemon pokemon: pokemons) {
            Document document = getDocument(pokemon);
            indexWriter.addDocument(document);
        }
    }

    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
    }

    public void buildIndexes()throws IOException {
        this.addPokemon();
        this.closeIndexWriter();
    }
}