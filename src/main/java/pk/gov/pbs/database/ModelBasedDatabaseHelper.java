package pk.gov.pbs.database;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import androidx.annotation.NonNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pk.gov.pbs.database.annotations.Default;
import pk.gov.pbs.database.annotations.NotNull;
import pk.gov.pbs.database.annotations.PrimaryKey;
import pk.gov.pbs.database.annotations.Unique;
import pk.gov.pbs.database.exceptions.UnsupportedDataType;
import pk.gov.pbs.utils.ExceptionReporter;

import static pk.gov.pbs.database.DatabaseUtils.getPrimaryKeyField;

public abstract class ModelBasedDatabaseHelper extends SQLiteOpenHelper {

    public ModelBasedDatabaseHelper(Context context, String dbName, int dbVersion) {
        super(context, dbName, null, dbVersion);
    }

    private Field[] getAllFields(Class<?> modelClass, boolean includePrivateFields){
        Map<String, Field> fieldsMap = new HashMap<>();
        for (Field field : modelClass.getDeclaredFields()) {
            if (includePrivateFields || !Modifier.isPrivate(field.getModifiers()))
                fieldsMap.put(field.getName(), field);
        }

        while (modelClass != Object.class){
            modelClass = modelClass.getSuperclass();
            if (modelClass == null)
                break;
            for (Field field : modelClass.getDeclaredFields()){
                if ((includePrivateFields || !Modifier.isPrivate(field.getModifiers())) && !fieldsMap.containsKey(field.getName()))
                    fieldsMap.put(field.getName(), field);
            }
        }

        Field[] fields = new Field[fieldsMap.values().size()];
        fieldsMap.values().toArray(fields);
        return fields;
    }

    @NonNull
    private Field[] getAllFields(Class<?> modelClass){
        return getAllFields(modelClass, false);
    }
    protected final String getSQLiteDataTypeFrom(Class<?> type) throws UnsupportedDataType {
        if (
                type == String.class
                        || type == char.class
                        || type == Character.class
        )
            return " TEXT ";
        else if (
                type == double.class
                        ||type == Double.class
                        || type == float.class
                        || type == Float.class
        )
            return " REAL ";
        else if (
                type == int.class
                        || type == Integer.class
                        || type == short.class
                        || type == Short.class
                        || type == long.class
                        || type == Long.class
        )
            return " INTEGER ";
        else if (
                type == boolean.class
                        || type == Boolean.class
        )
            return " BOOLEAN ";
        else if (
                type == byte[].class
                        || type == Byte[].class
        )
            return " BLOB ";
        else
            throw new UnsupportedDataType(type);
    }

    protected final ContentValues getContentValuesFromModel(Object o){
        ContentValues values = new ContentValues();
        for (Field field : o.getClass().getFields()){
            try {
                if (!field.isAccessible())
                    field.setAccessible(true);

                values.put(field.getName(), field.get(o) != null ? field.get(o).toString() : null);
            } catch (Exception e) {
                ExceptionReporter.printStackTrace(e);
            }
        }
        return values;
    }

    public final <T> T extractObjectFromCursor(Class<T> type, Cursor c) throws IllegalAccessException, InstantiationException {
        T o = type.newInstance();
        for (Field f : type.getFields()){
            if (!Modifier.isPrivate(f.getModifiers())) {
                if (c.getColumnIndex(f.getName()) == -1)
                    continue;

                if (!f.isAccessible())
                    f.setAccessible(true);

                switch (f.getType().getSimpleName()) {
                    case "char":
                    case "Character":
                    case "String":
                        f.set(o, c.getString(c.getColumnIndex(f.getName())));
                        break;
                    case "int":
                        f.set(o, c.getInt(c.getColumnIndex(f.getName())));
                        break;
                    case "Integer":
                        f.set(o, c.isNull(c.getColumnIndex(f.getName())) ? null :
                                c.getInt(c.getColumnIndex(f.getName()))
                        );
                        break;
                    case "long":
                        f.set(o, c.getLong(c.getColumnIndex(f.getName())));
                        break;
                    case "Long":
                        f.set(o, c.isNull(c.getColumnIndex(f.getName())) ? null :
                                c.getLong(c.getColumnIndex(f.getName()))
                        );
                        break;
                    case "double":
                        f.set(o, c.getDouble(c.getColumnIndex(f.getName())));
                        break;
                    case "Double":
                        f.set(o, c.isNull(c.getColumnIndex(f.getName())) ? null :
                                c.getDouble(c.getColumnIndex(f.getName()))
                        );
                        break;
                    case "boolean":
                        f.set(o, c.getInt(c.getColumnIndex(f.getName())) == 1);
                        break;
                    case "Boolean":
                        f.set(o, c.isNull(c.getColumnIndex(f.getName())) ? null :
                                c.getInt(c.getColumnIndex(f.getName())) == 1
                        );
                        break;
                    case "float":
                        f.set(o, c.getFloat(c.getColumnIndex(f.getName())));
                        break;
                    case "Float":
                        f.set(o, c.isNull(c.getColumnIndex(f.getName())) ? null :
                                c.getFloat(c.getColumnIndex(f.getName()))
                        );
                        break;
                    case "short":
                        f.set(o, c.getShort(c.getColumnIndex(f.getName())));
                        break;
                    case "Short":
                        f.set(o, c.isNull(c.getColumnIndex(f.getName())) ? null :
                                c.getShort(c.getColumnIndex(f.getName()))
                        );
                        break;
                    case "byte[]":
                        f.set(o, c.getBlob(c.getColumnIndex(f.getName())));
                        break;
                    case "Byte[]":
                        f.set(o, c.isNull(c.getColumnIndex(f.getName())) ? null :
                                c.getBlob(c.getColumnIndex(f.getName()))
                        );
                        break;
                }
            }
        }
        return o;
    }

    protected final <T> T extractFieldFromCursor(Class<T> type, Cursor c, Integer columnIndex) throws ClassCastException {
        T o = null;

        switch (type.getSimpleName()) {
            case "char":
            case "Character":
            case "String":
                o = (T) String.valueOf(c.getString(columnIndex));
                break;
            case "Integer":
                o = c.isNull(columnIndex) ? null :
                        (T) Integer.valueOf(c.getInt(columnIndex));
            case "int":
                o = (T) Integer.valueOf(c.getInt(columnIndex));
                break;
            case "Long":
                o = c.isNull(columnIndex) ? null :
                        (T) Long.valueOf(c.getLong(columnIndex));
            case "long":
                o = (T) Long.valueOf(c.getLong(columnIndex));
                break;
            case "Double":
                o = c.isNull(columnIndex) ? null :
                        (T) Double.valueOf(c.getDouble(columnIndex));
            case "double":
                o = (T) Double.valueOf(c.getDouble(columnIndex));
                break;
            case "Boolean":
                o = c.isNull(columnIndex) ? null :
                        (T) Boolean.valueOf(c.getInt(columnIndex)==1);
            case "boolean":
                o = (T) Boolean.valueOf(c.getInt(columnIndex)==1);
                break;
            case "Float":
                o = c.isNull(columnIndex) ? null :
                        (T) Float.valueOf(c.getFloat(columnIndex));
            case "float":
                o = (T) Float.valueOf(c.getFloat(columnIndex));
                break;
            case "Short":
                o = c.isNull(columnIndex) ? null :
                        (T) Short.valueOf(c.getShort(columnIndex));
            case "short":
                o = (T) Short.valueOf(c.getShort(columnIndex));
                break;
            case "Byte[]":
                o = c.isNull(columnIndex) ? null :
                        (T) c.getBlob(columnIndex);
            case "byte[]":
                o = (T) c.getBlob(columnIndex);
                break;
        }
        return o;
    }

    protected void createTable(Class<?> modelClass, SQLiteDatabase db) throws UnsupportedDataType {
        HashMap<String, ArrayList<String>> uniqueConstraint = new HashMap<>();
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("CREATE TABLE ").append(modelClass.getSimpleName()).append(" (");
        for (Field field : getAllFields(modelClass)){
            if (Modifier.isPublic(field.getModifiers()) || Modifier.isProtected(field.getModifiers())) {
                queryBuilder.append(field.getName())
                        .append(getSQLiteDataTypeFrom(field.getType()));
                PrimaryKey pk = field.getAnnotation(PrimaryKey.class);
                if (pk != null){
                    queryBuilder.append("PRIMARY KEY")
                            .append(pk.autogenerate() ? " AUTOINCREMENT" : " ");
                }
                Default ano = field.getAnnotation(Default.class);
                if (ano != null){
                    queryBuilder.append("DEFAULT ")
                            .append('\'').append(ano.value()).append('\'');
                }
                queryBuilder.append(field.getAnnotation(NotNull.class) == null ? "," : " NOT NULL,");
            }
            Unique uAno = field.getAnnotation(Unique.class);
            if (uAno != null){
                if (uniqueConstraint.containsKey(uAno.index())){
                    uniqueConstraint.get(uAno.index()).add(field.getName());
                } else {
                    ArrayList<String> cols = new ArrayList<>();
                    cols.add(field.getName());
                    uniqueConstraint.put(uAno.index(), cols);
                }
            }
        }
        queryBuilder.deleteCharAt(queryBuilder.length()-1).append(")");
        db.execSQL(queryBuilder.toString());
        if (uniqueConstraint.size() > 0) {
            for (String index : uniqueConstraint.keySet()){
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE UNIQUE INDEX `")
                        .append(modelClass.getSimpleName()).append("_unique_index_").append(index).append('`')
                        .append(" ON `").append(modelClass.getSimpleName()).append("` (");
                for (String col :  uniqueConstraint.get(index)){
                    sb.append('`').append(col).append("`,");
                }
                sb.deleteCharAt(sb.length()-1).append(")");
                db.execSQL(sb.toString());
            }
        }
    }

    protected void dropTable(Class<?> modelClass, SQLiteDatabase db){
        db.execSQL("DROP TABLE IF EXISTS " + modelClass.getSimpleName());
    }

    public <T> DatabaseOperationExecutor<T> executeDatabaseOperation(IDatabaseOperation<T> databaseRead){
        DatabaseOperationExecutor<T> operationExecutor = new DatabaseOperationExecutor<T>(databaseRead);
        operationExecutor.execute(ModelBasedDatabaseHelper.this);
        return operationExecutor;
    }

    public Long insertOrThrow(@NonNull Object model) throws SQLException {
        return getWritableDatabase()
                .insertOrThrow(
                        model.getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(model)
                );
    }

    public List<Long> insertOrThrow(@NonNull Object[] models) throws SQLException {
        List<Long> result = new ArrayList<>();
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try{
            for (int i = 0; i < models.length; i++){
                result.add(db.insertOrThrow(
                        models[i].getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(models[i])
                ));
            }
            db.setTransactionSuccessful();
        } catch (SQLException sqlException){
            db.endTransaction();
            throw sqlException;
        } finally {
            db.endTransaction();
        }
        return result;
    }

    public Long insert(@NonNull Object model){
        return getWritableDatabase()
                .insert(
                        model.getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(model)
                );
    }

    public List<Long> insert(Object[] models){
        List<Long> result = new ArrayList<>();
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try{
            for (int i = 0; i < models.length; i++){
                result.add(db.insertOrThrow(
                        models[i].getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(models[i])
                ));
            }
            db.setTransactionSuccessful();
        } catch (SQLException sqlException){
            ExceptionReporter.printStackTrace(sqlException);
        } finally {
            db.endTransaction();
        }
        return result;
    }

    public Long replaceOrThrow(@NonNull Object model) throws SQLException{
        return getWritableDatabase()
                .replaceOrThrow(
                        model.getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(model)
                );
    }

    public List<Long> replaceOrThrow(Object[] models) throws SQLException{
        List<Long> result = new ArrayList<>();
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try{
            for (int i = 0; i < models.length; i++){
                result.add(db.replaceOrThrow(
                        models[i].getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(models[i])
                ));
            }
            db.setTransactionSuccessful();
        } catch (SQLException sqlException){
            db.endTransaction();
            throw sqlException;
        } finally {
            db.endTransaction();
        }
        return result;
    }

    public Long replace(@NonNull Object model){
        return getWritableDatabase()
                .replace(
                        model.getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(model)
                );
    }

    public List<Long> replace(Object[] models){
        List<Long> ids = new ArrayList<>();
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try{
            for (int i = 0; i < models.length; i++){
                ids.add(db.replaceOrThrow(
                        models[i].getClass().getSimpleName(),
                        null,
                        getContentValuesFromModel(models[i])
                ));
            }
            db.setTransactionSuccessful();
        } catch (SQLException sqlException){
            db.endTransaction();
            ExceptionReporter.printStackTrace(sqlException);
        } finally {
            db.endTransaction();
        }
        return ids;
    }

    public Integer update(Object object) throws SQLException, IllegalAccessException {
        Field pk = getPrimaryKeyField(object.getClass());

        if (pk == null)
            throw new SQLException("Provided object has no primary key, Can not proceed to update record");

        return getWritableDatabase().update(
                object.getClass().getSimpleName(),
                getContentValuesFromModel(object),
                pk.getName() + "= ?",
                new String[]{ pk.get(object).toString() }
        );
    }

    public <T> List<T> query(Class<?> outputType, String... args){
        String sql = "SELECT * FROM `"+outputType.getSimpleName()+"`";
        if (args != null && args.length > 0) {
            sql += " WHERE " + args[0];

            if (args.length == 1)
                return (List<T>) queryRawSql(outputType, sql, null);

            String[] arg = new String[args.length - 1];
            System.arraycopy(args, 1, arg, 0, args.length - 1);
            return (List<T>) queryRawSql(outputType, sql, arg);
        }
        return (List<T>) queryRawSql(outputType, sql, null);
    }

    public <T> List<T> queryRawSql(Class<T> outputType, String rawSql, String... selectionArgs) {
        List<T> result = new ArrayList<T>();

        rawSql = rawSql.toLowerCase();
        if (rawSql.contains("<table>"))
            rawSql = rawSql.replace("<table>", "`" + outputType.getSimpleName() + "`");

        Cursor c = getReadableDatabase().rawQuery(rawSql, selectionArgs);
        if (c.moveToFirst()){
            do {
                try {
                    result.add(extractObjectFromCursor(outputType, c));
                } catch (IllegalAccessException e) {
                    ExceptionReporter.printStackTrace(e);
                } catch (InstantiationException e) {
                    ExceptionReporter.printStackTrace(e);
                }
            } while(c.moveToNext());
        }
        c.close();
        return result;
    }

    public <T> T querySingle(Class<T> outputType,String selectionCriteria,String... selectionArgs) {
        String sql = "SELECT * FROM `"+outputType.getSimpleName()+"` WHERE " + selectionCriteria;
        return (T) querySingleRawSql(outputType, sql, selectionArgs);
    }

    public <T> T querySingleRawSql(Class<T> outputType, String rawSql, String... selectionArgs) {
        T result = null;

        rawSql = rawSql.toLowerCase();
        if (rawSql.contains("<table>"))
            rawSql = rawSql.replace("<table>", "`" + outputType.getSimpleName() + "`");

        Cursor c = getReadableDatabase().rawQuery(rawSql, selectionArgs);
        if (c.moveToFirst()){
            do {
                try {
                    result = extractObjectFromCursor(outputType, c);
                } catch (IllegalAccessException e) {
                    ExceptionReporter.printStackTrace(e);
                } catch (InstantiationException e) {
                    ExceptionReporter.printStackTrace(e);
                }
            } while(c.moveToNext());
        }
        c.close();
        return result;
    }

    public List<Map<String, String>> queryRowsAsMap(String sql, String... selectionArgs) {
        List<Map<String, String>> result = new ArrayList<>();
        Cursor c = getReadableDatabase().rawQuery(sql, selectionArgs);
        if (c.moveToFirst()){
            do {
                Map<String, String> row = new HashMap<>();
                for (String col : c.getColumnNames())
                    row.put(col, c.getString(c.getColumnIndex(col)));
                result.add(row);
            } while(c.moveToNext());
        }
        c.close();
        return result;
    }

    public List<String[]> queryRowsAsList(String sql, String... selectionArgs) {
        List<String[]> result = new ArrayList<>();
        Cursor c = getReadableDatabase().rawQuery(sql, selectionArgs);
        if (c.moveToFirst()){
            result.add(c.getColumnNames());
            do {
                String[] row = new String[c.getColumnCount()];
                for (int i = 0; i < c.getColumnCount(); i++)
                    row[i] = c.getString(c.getColumnIndex(result.get(0)[i]));
                result.add(row);
            } while(c.moveToNext());
        }
        c.close();
        return result;
    }

    public <T> List<T[]> queryRowsWith(String sql, Extractor<T> extractor, String... selectionArgs) {
        List<T[]> result = new ArrayList<>();
        Cursor c = getReadableDatabase().rawQuery(sql, selectionArgs);
        if (c.moveToFirst()){
            do {
                List<T> row = new ArrayList<>();
                for (int i = 0; i < c.getColumnCount(); i++)
                    row.add(extractor.extract(c, i));
                result.add((T[]) row.toArray());
            } while(c.moveToNext());
        }
        c.close();
        return result;
    }

    public <K, V> HashMap<K, V> queryRowsMapped(String mapKey, Class<V> outputType, String... args) throws NoSuchFieldException {
        String sql = "SELECT * FROM `" + outputType.getSimpleName() + "`";
        if (args != null && args.length > 0) {
            sql += " WHERE " + args[0];

            if (args.length == 1)
                return queryRowsMappedRawSQL(mapKey, outputType, sql, (String) null);

            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, args.length - 1);
            return queryRowsMappedRawSQL(mapKey, outputType, sql, newArgs);
        }

        return queryRowsMappedRawSQL(mapKey, outputType, sql, (String) null);
    }

    public  <K,V> HashMap<K,V> queryRowsMappedRawSQL(String mapKey, Class<V> outputType, String rawSql, String... selectionArgs) throws NoSuchFieldException {
        HashMap<K, V> result = new HashMap<>();
        Field keyField = mapKey == null ? DatabaseUtils.getPrimaryKeyField(outputType)
                : outputType.getField(mapKey);

        rawSql = rawSql.toLowerCase();
        if (rawSql.contains("<table>"))
            rawSql = rawSql.replace("<table>", "`" + outputType.getSimpleName() + "`");

        Cursor c = getReadableDatabase().rawQuery(rawSql, selectionArgs);
        if (c.moveToFirst()){
            do {
                try {
                    V obj = extractObjectFromCursor(outputType, c);
                    result.put((K) keyField.get(obj), obj);
                } catch (IllegalAccessException e) {
                    ExceptionReporter.printStackTrace(e);
                } catch (InstantiationException e) {
                    ExceptionReporter.printStackTrace(e);
                }
            } while(c.moveToNext());
        }
        c.close();
        return result;
    }

    public <K, V> HashMap<K, List<V>> selectGroupedRows(String mapKey, Class<V> outputType, String selectionCriteria, String[] selectionArgs) throws NoSuchFieldException {
        String sql = "SELECT * FROM `" + outputType.getSimpleName() + "`";
        if (selectionCriteria != null && !selectionCriteria.isEmpty())
            sql += " WHERE " + selectionCriteria;
        return selectGroupedRowsRawSQL(mapKey, outputType, sql, selectionArgs);
    }

    public  <K,V> HashMap<K, List<V>> selectGroupedRowsRawSQL(String mapKey, Class<V> outputType, String rawSql, String[] selectionArgs) throws NoSuchFieldException {
        HashMap<K, List<V>> result = new HashMap<>();
        Field keyField = outputType.getField(mapKey);

        rawSql = rawSql.toLowerCase();
        if (rawSql.contains("<table>"))
            rawSql = rawSql.replace("<table>", "`" + outputType.getSimpleName() + "`");

        Cursor c = getReadableDatabase().rawQuery(rawSql, selectionArgs);
        if (c.moveToFirst()){
            do {
                try {
                    V obj = extractObjectFromCursor(outputType, c);
                    if (keyField.get(obj) != null) {
                        if (result.containsKey((K) keyField.get(obj))) {
                            result.get((K) keyField.get(obj)).add(obj);
                        } else {
                            List<V> vList = new ArrayList<>();
                            vList.add(obj);
                            result.put((K) keyField.get(obj), vList);
                        }
                    }
                } catch (IllegalAccessException e) {
                    ExceptionReporter.printStackTrace(e);
                } catch (InstantiationException e) {
                    ExceptionReporter.printStackTrace(e);
                }
            } while(c.moveToNext());
        }
        c.close();
        return result;
    }

    public interface Extractor<T> {
        T extract(Cursor cursor, int columnIndex);
    }
}
