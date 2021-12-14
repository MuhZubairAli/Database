package pk.gov.pbs.database;

public interface IDatabaseOperation<T> {
    T execute (ModelBasedDatabaseHelper db);
    void postExecute(T result);
}
