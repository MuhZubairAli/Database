package pk.gov.pbs.database;

import android.database.Cursor;

import androidx.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import pk.gov.pbs.database.annotations.PrimaryKey;
import pk.gov.pbs.database.annotations.Unique;
import pk.gov.pbs.database.exceptions.ColumnNotFound;
import pk.gov.pbs.utils.ExceptionReporter;

public class DatabaseUtils {
    public static Field getPrimaryKeyField(Class<?> model){
        for (Field field : model.getFields()){
            if (field.getAnnotation(PrimaryKey.class) != null){
                return field;
            }
        }
        return null;
    }

    public static List<Field> getUniqueKeyFields(Class<?> model, @Nullable String key){
        if (key == null) key = "default";
        HashMap<String, ArrayList<Field>> uniqueKeys = getUniqueKeyFields(model);
        if (uniqueKeys != null && uniqueKeys.containsKey(key)) {
            return uniqueKeys.get(key);
        }
        return null;
    }

    public static HashMap<String, ArrayList<Field>> getUniqueKeyFields(Class<?> model){
        HashMap<String, ArrayList<Field>> uniqueConstraint = new HashMap<>();
        for (Field field : model.getFields()){
            Unique uAno = field.getAnnotation(Unique.class);
            if (uAno != null){
                if (uniqueConstraint.containsKey(uAno.index())){
                    uniqueConstraint.get(uAno.index()).add(field);
                } else {
                    ArrayList<Field> cols = new ArrayList<>();
                    cols.add(field);
                    uniqueConstraint.put(uAno.index(), cols);
                }
            }
        }

        if (!uniqueConstraint.isEmpty())
            return uniqueConstraint;
        return null;
    }

    public static Field[] getAllFields(Class<?> modelClass, boolean includePrivateFields){
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

    public static String[] getAllColumnNames(Class<?> model){
        Field[] fields = model.getFields();
        String[] cols = new String[fields.length];

        for (int i = 0; i < fields.length; i++){
            cols[i] = fields[i].getName();
        }

        return cols;
    }
    
    public static List<String[]> extractFromCursor(Cursor cursor) {
        List<String[]> result = new ArrayList<>();
        if (cursor.moveToFirst()){
            result.add(cursor.getColumnNames());
            do {
                String[] row = new String[cursor.getColumnCount()];
                for (int i = 0; i < cursor.getColumnCount(); i++)
                    row[i] = cursor.getString(cursor.getColumnIndex(result.get(0)[i]));
                result.add(row);
            } while(cursor.moveToNext());
        }
        return result;
    }

    public static Map<String, String[]> extractFromCursorMapped(String key, Cursor cursor) throws ColumnNotFound {
        if (cursor.getColumnIndex(key) == -1)
            throw new ColumnNotFound("Specified column '"+key+"' does not exists in cursor", cursor);

        Map<String, String[]> result = new HashMap<>();
        int keyIndex = cursor.getColumnIndex(key);

        if (cursor.moveToFirst()){
            do {
                String[] row = new String[cursor.getColumnCount()];
                for (int i = 0; i < cursor.getColumnCount(); i++)
                    row[i] = cursor.getString(i);
                result.put(cursor.getString(keyIndex), row);
            } while(cursor.moveToNext());
        }
        return result;
    }

    public static HashMap<String,String> extractMapFromCursor(Cursor c) {
        HashMap<String,String> result = new HashMap<>();
        for (String column : c.getColumnNames()){
            result.put(column, c.getString(c.getColumnIndex(column)));
        }
        return result;
    }

    public static <T> T extractObjectFromCursor(Class<T> type, Cursor c, boolean includePrivateFields) throws IllegalAccessException, InstantiationException {
        T o = type.newInstance();
        for (Field f : getAllFields(type, includePrivateFields)){
            if (includePrivateFields || !Modifier.isPrivate(f.getModifiers())) {
                if (c.getColumnIndex(f.getName()) == -1)
                    continue;

                if (!f.isAccessible())
                    f.setAccessible(true);

                switch (f.getType().getSimpleName()) {
                    case "char":
                    case "Character":
                    case "char[]":
                    case "Character[]":
                    case "CharSequence":
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
                    case "byte":
                    case "short":
                        f.set(o, c.getShort(c.getColumnIndex(f.getName())));
                        break;
                    case "Byte":
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

    public static <T> T extractFieldFromCursor(Class<T> type, Cursor c, Integer columnIndex) throws ClassCastException {
        T o = null;
        switch (type.getSimpleName()) {
            case "char":
            case "Character":
            case "char[]":
            case "Character[]":
            case "CharSequence":
            case "String":
                o = (T) c.getString(columnIndex);
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
            case "Byte":
            case "Short":
                o = c.isNull(columnIndex) ? null :
                        (T) Short.valueOf(c.getShort(columnIndex));
            case "byte":
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

    /**
     * get the result from future for database query
     * if database is still processing the query then caller thread
     * will wait to join database executor service and then gets result
     * @param future future returned by repository method or received from executor service
     *               after submit database lambda
     * @param <T> type of result
     * @return returns the actual result of query of type T
     */
    public static <T> T getFutureValue(Future<T> future){
        try{
            return future.get();
        } catch (InterruptedException e) {
            ExceptionReporter.handle(e);
        } catch (ExecutionException e) {
            ExceptionReporter.handle(e);
        }
        return null;
    }
}
