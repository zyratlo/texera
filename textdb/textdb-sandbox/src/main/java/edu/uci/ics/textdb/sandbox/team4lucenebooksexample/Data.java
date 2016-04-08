package edu.uci.ics.textdb.sandbox.team4lucenebooksexample;

public class Data {

    private static final Books[] BOOKS = {
            new Book(
                    "1",
                    "Hôtel Rivoli",
                    "Paris",
                    "If you like historical Paris, you will adore the Book. The Book is right in the center of the city, right beside the Louvre, residence of the Kings of France during several centuries and, today, the greatest museum in the world."),
            new Book(
                    "40",
                    " Grand Book",
                    "Avignon",
                    " New Book complex, offering comfortable, elegant and spacious suites and apartments, facing the old city ramparts, just steps away from the center of Avignon."),

    };

    public static Book[] getBooks() {
        return BOOKS;
    }

    public static Book getBook(String id) {
        for (Book book : BOOKS) {
            if (id.equals(book.getId())) {
                return book;
            }
        }
        return null;
    }
}
