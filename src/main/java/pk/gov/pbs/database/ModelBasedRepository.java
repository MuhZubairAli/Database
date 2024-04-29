package pk.gov.pbs.database;

import static pk.gov.pbs.database.DatabaseUtils.extractFieldFromCursor;

import android.app.Application;
import android.database.Cursor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import pk.gov.pbs.utils.ExceptionReporter;
import pk.gov.pbs.utils.StaticUtils;

public abstract class ModelBasedRepository {
    private static Application mContext;
    protected static final int THREAD_COUNT = 4;
    protected static final ExecutorService dbExecutorService = Executors
            .newFixedThreadPool(THREAD_COUNT);

    protected ModelBasedRepository(Application context){
        mContext = context;
    }

    public Application getContext() {
        return mContext;
    }

    public ExecutorService getExecutorService(){
        return dbExecutorService;
    }

    public <T> DatabaseOperationExecutor<T> executeDatabaseOperation(IDatabaseOperation<T> databaseRead){
        return getDatabase().executeDatabaseOperation(databaseRead);
    }

    public Future<Long> executeDatabaseWrite(IDatabaseWrite dbWrite){
        return dbExecutorService.submit(() ->
                dbWrite.doDatabaseWriteOperation(getDatabase())
        );
    }

    public Future<Long> executeDatabaseWrite(IDatabaseWrite dbWrite, IOnSuccess successCallback){
        return dbExecutorService.submit(() -> {
            Long insertID = dbWrite.doDatabaseWriteOperation(getDatabase());
            successCallback.onSuccess(StaticUtils.getHandler(), insertID);
            return insertID;
        });
    }

    public <T> Future<T> querySingle(Class<T> outputType, String selectionCriteria, String... args){
        return getExecutorService().submit(
                () -> getDatabase().querySingle(outputType,selectionCriteria,args)
        );
    }

    public <K, V> Future<Map<K, V>> queryRowsMapped(String mapKey, Class<V> outputClass, String... selectionArgs){
        return dbExecutorService.submit(()->{
            try {
                return getDatabase().queryRowsMapped(mapKey, outputClass, selectionArgs);
            } catch (Exception e) {
                ExceptionReporter.handle(e);
                return null;
            }
        });
    }

    public <T> Future<T> selectColAs(Class<T> outputType, String sql, String... args){
        return dbExecutorService.submit(
                () -> {
                    T result = null;
                    Cursor cursor = getDatabase().getReadableDatabase().rawQuery(sql, args);
                    if (cursor.moveToFirst()) {
                        result = extractFieldFromCursor(outputType, cursor, 0);
                    }
                    cursor.close();
                    return result;
                }
        );
    }

    public <T> Future<List<T>> selectColMultiAs(Class<T> outputType, String sql, String... args){
        return getExecutorService().submit(
                () -> {
                    Cursor cursor = getDatabase().getReadableDatabase().rawQuery(sql, args);
                    List<T> result = new ArrayList<>();
                    if (cursor.moveToFirst()) {
                        do {
                            result.add(extractFieldFromCursor(outputType, cursor, 0));
                        } while (cursor.moveToNext());
                        cursor.close();
                        return result;
                    }
                    return null;
                }
        );
    }

    public <T> Future<List<T>> query(Class<T> outputType, String... args){
        return getExecutorService().submit(
                () -> getDatabase().query(outputType,args)
        );
    }

    /**
     * Insert convenience methods with overloads
     */

    public Future<Long> insert(Object object){
        return dbExecutorService.submit(
                () -> getDatabase().insert(object)
        );
    }

    public Future<List<Long>> insert(Object[] object){
        return dbExecutorService.submit(
                () -> getDatabase().insert(object)
        );
    }

    /**
     * Update convenience methods with overloads
     */

    public Future<Integer> update(Object object){
        return dbExecutorService.submit(
                () -> getDatabase().update(object)
        );
    }

    /**
     * Replace convenience methods with overloads
     */

    public Future<Long> replace(Object object){
        return dbExecutorService.submit(
                () -> getDatabase().replace(object)
        );
    }

    public Future<List<Long>> replace(Object[] object){
        return dbExecutorService.submit(
                () -> getDatabase().replace(object)
        );
    }

    public Future<Long> replaceOrThrow(Object object){
        return dbExecutorService.submit(
                () -> getDatabase().replaceOrThrow(object)
        );
    }

    public Future<List<Long>> replaceOrThrow(Object[] object){
        return dbExecutorService.submit(
                () -> getDatabase().replaceOrThrow(object)
        );
    }

    public abstract ModelBasedDatabaseHelper getDatabase();
}
