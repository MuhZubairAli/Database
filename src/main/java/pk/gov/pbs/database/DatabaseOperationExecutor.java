package pk.gov.pbs.database;

import android.os.AsyncTask;

public class DatabaseOperationExecutor<T> extends AsyncTask<ModelBasedDatabaseHelper, Void, T> {
    IDatabaseOperation<T> dbOperation;
    public DatabaseOperationExecutor(IDatabaseOperation<T> operation){
        dbOperation = operation;
    }

    @Override
    protected final T doInBackground(ModelBasedDatabaseHelper... modelBasedDatabaseHelpers) {
        return dbOperation.execute(modelBasedDatabaseHelpers[0]);
    }

    @Override
    protected final void onPostExecute(T list) {
        dbOperation.postExecute(list);
    }
}
