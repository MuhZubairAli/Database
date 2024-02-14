package pk.gov.pbs.database;

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
        if (uniqueConstraint.size() > 0)
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
            ExceptionReporter.printStackTrace(e);
        } catch (ExecutionException e) {
            ExceptionReporter.printStackTrace(e);
        }
        return null;
    }
}
