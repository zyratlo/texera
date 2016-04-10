package edu.uci.ics.textdb.sandbox.team2luceneexample;

/**
 * Created by kishorenarendran on 06/04/16.
 */
public class Pokemon {
    public int id;
    public String name;
    public String moves;
    public String types;

    public Pokemon(int id, String name, String moves, String types) {
        this.id = id;
        this.name = name;
        this.moves = moves;
        this.types = types;
        System.out.println(name + " : " + types);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMoves() {
        return moves;
    }

    public void setMoves(String moves) {
        this.moves = moves;
    }

    public String getTypes() {
        return types;
    }

    public void setTypes(String types) {
        this.types = types;
    }
}
