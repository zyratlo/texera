package edu.uci.ics.textdb.sandbox.team2luceneexample;

/**
 * Created by shiladityasen on 06/04/16.
 */

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;

//import edu.uci.ics.textdb.sandbox.team2luceneexample.LuceneConstants;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Paths;
import java.util.Arrays;

import java.io.IOException;
import java.util.ArrayList;

public class Pokedexer {
    private Document getDocument(Pokemon pokemon) throws IOException {
        Document doc = new Document();

        //index pokemon info
        Field id = new IntField(LuceneConstants.ID_FIELD, pokemon.getId(), Field.Store.YES);
        Field name = new StringField(LuceneConstants.NAME_FIELD, pokemon.getName(), Field.Store.NO);

        ArrayList<String> all_types = new ArrayList<String>(Arrays.asList(pokemon.getTypes().split("\\s*,\\s*")));
        ArrayList<Field> types = new ArrayList();
        for (int i = 0; i < types.size(); i++)
            types.add(new StringField(LuceneConstants.MOVES_FIELD, all_types.get(i), Field.Store.YES));

        ArrayList<String> all_moves = new ArrayList(Arrays.asList(pokemon.getMoves().split("\\s*,\\s*")));
        ArrayList<Field> moves = new ArrayList();
        for (int i = 0; i < moves.size(); i++)
            moves.add(new StringField(LuceneConstants.MOVES_FIELD, all_moves.get(i), Field.Store.NO));

        //add fields to doc
        doc.add(id);
        doc.add(name);
        for (int i = 0; i < types.size(); i++)
            doc.add(types.get(i));
        for (int i = 0; i < moves.size(); i++)
            doc.add(moves.get(i));

        return doc;
    }


    private IndexWriter ix_writer;

    public Pokedexer(String indexDirectoryPath) throws IOException {
        Directory direc = FSDirectory.open(Paths.get(indexDirectoryPath));
        IndexWriterConfig ix_write_conf = new IndexWriterConfig(new StandardAnalyzer());

        ix_writer = new IndexWriter(direc, ix_write_conf);
    }


    public void addPokemon(Pokemon pokemon) throws IOException {
        Document doc = getDocument(pokemon);
        ix_writer.addDocument(doc);
    }

}