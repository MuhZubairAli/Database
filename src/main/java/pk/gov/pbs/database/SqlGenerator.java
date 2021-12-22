package pk.gov.pbs.database;

import com.google.gson.annotations.SerializedName;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import pk.gov.pbs.database.annotations.Default;
import pk.gov.pbs.database.annotations.NotNull;
import pk.gov.pbs.database.annotations.SqlDataType;
import pk.gov.pbs.database.annotations.SqlExclude;
import pk.gov.pbs.database.annotations.SqlPrimaryKey;
import pk.gov.pbs.database.annotations.Table;
import pk.gov.pbs.database.annotations.Unique;

public class SqlGenerator {
    private final String defaultSchema = "[dbo]";

    protected String makeStringTableWithSchema(String tableName){
        return String.format("%s.[%s]",defaultSchema,tableName);
    }

    protected String enCap(String str){
        return "[".concat(str).concat("]");
    }

    public String getSqlDataTypeFrom(Class<?> type) {
        if (
                type == String.class
                        || type == CharSequence.class
                        || type == char[].class
                        || type == Character[].class
        )
            return "[VARCHAR] (256)";
        else if (
                type == double.class
                        ||type == Double.class
                        || type == float.class
                        || type == Float.class
        )
            return "[FLOAT]";
        else if (
                type == int.class
                        || type == Integer.class
        )
            return "[INT]";
        else if (
                 type == short.class
                || type == Short.class
        )
            return "[TINYINT]";
        else if (
                type == long.class
                || type == Long.class
        )
            return "[BIGINT]";
        else if (
                type == boolean.class
                        || type == Boolean.class
        )
            return "[BIT]";
        else if (
                type == byte.class
                || type == Byte.class
        )
            return "[TINYINT]";

        return "[BINARY]";
    }

    public String generateSqlCreateTable(Class<?> modelClass, boolean addUnixTimestamps, boolean includeUnixTsFunction) {
        HashMap<String, ArrayList<String>> uniqueConstraint = new HashMap<>();
        ArrayList<String> primaryKeyConstraint = new ArrayList<>();

        StringBuilder queryBuilder = new StringBuilder();

        if (includeUnixTsFunction){
            queryBuilder.append(
                    "DROP FUNCTION IF EXISTS "+defaultSchema+".[UNIX_TS]\n" +
                            "GO\n\n"+
                    "CREATE FUNCTION "+defaultSchema+".[UNIX_TS] ()\n" +
                    "RETURNS BIGINT\n" +
                    "AS \n" +
                    "BEGIN\n" +
                    "  RETURN DATEDIFF_BIG(SECOND,{d '1970-01-01'}, GETDATE())\n" +
                    "END\n" +
                    "GO\n\n");

        }

        Table table = modelClass.getAnnotation(Table.class);
        String tableName = (table != null && !table.name().isEmpty()) ? makeStringTableWithSchema(table.name())
                : makeStringTableWithSchema(modelClass.getSimpleName());

        queryBuilder.
                append("DROP TABLE IF EXISTS ").
                append(tableName).
                append("\nGO\n\n").
                append("CREATE TABLE ").
                append(tableName).
                append(" (\n");

        for (Field field : modelClass.getFields()){
            SerializedName name = field.getAnnotation(SerializedName.class);
            SqlPrimaryKey pk = field.getAnnotation(SqlPrimaryKey.class);
            Unique uAno = field.getAnnotation(Unique.class);
            SqlDataType type = field.getAnnotation(SqlDataType.class);
            SqlExclude exclude = field.getAnnotation(SqlExclude.class);

            if (exclude != null)
                continue;

            if (Modifier.isPublic(field.getModifiers()) || Modifier.isProtected(field.getModifiers())) {
                //COLUMN NAME
                queryBuilder.append((name == null) ? enCap(field.getName()) : enCap(name.value()));

                //COLUMN DATA TYPE
                queryBuilder.append(' ')
                        .append((type==null) ? getSqlDataTypeFrom(field.getType()) : type.value());

                // set identity params if pk
                if (pk != null){
                    if (pk.autogenerate()) {
                        queryBuilder.
                                append(" IDENTITY(").
                                append(pk.seed()).
                                append(",").
                                append(pk.increment()).
                                append(") NOT NULL,\n");
                    } else
                        queryBuilder.append(" NOT NULL,\n");
                    primaryKeyConstraint.add((name == null) ? enCap(field.getName()) : enCap(name.value()));
                    continue;
                }

                //COLUMN NULLITY
                queryBuilder.append(field.getAnnotation(NotNull.class) == null ? " NULL" : " NOT NULL");

                //COLUMN DEFAULT VALUE
                Default aDefault = field.getAnnotation(Default.class);
                if (aDefault != null){
                    queryBuilder.append(" DEFAULT '").append(aDefault.value()).append("',\n");
                } else
                    queryBuilder.append(",\n");
            }

            if (uAno != null){
                if (uniqueConstraint.containsKey(uAno.index())){
                    uniqueConstraint.get(uAno.index()).add((name == null) ? enCap(field.getName()) : enCap(name.value()));
                } else {
                    ArrayList<String> cols = new ArrayList<>();
                    cols.add((name == null) ? enCap(field.getName()) : enCap(name.value()));
                    uniqueConstraint.put(uAno.index(), cols);
                }
            }
        }

        if (addUnixTimestamps) {
            queryBuilder.
                    append("[TSCreated] [BIGINT] NOT NULL DEFAULT [dbo].UNIX_TS (),\n").
                    append("[TSUpdated] [BIGINT] NULL,\n");
        }

        if (primaryKeyConstraint.size() > 0){
            queryBuilder.
                    append("CONSTRAINT ").
                    append(enCap("PK_"+modelClass.getSimpleName())).
                    append(" PRIMARY KEY CLUSTERED (\n");
            for (String key : primaryKeyConstraint)
                queryBuilder.append(key).append(" ASC, ");

            queryBuilder.
                    deleteCharAt(queryBuilder.lastIndexOf(",")).
                    append("\n) WITH (\n").
                    append("PAD_INDEX = OFF, ").append("STATISTICS_NORECOMPUTE = OFF, ")
                    .append("IGNORE_DUP_KEY = OFF, ").append("ALLOW_ROW_LOCKS = ON, ")
                    .append("ALLOW_PAGE_LOCKS = ON").append("\n) ON [PRIMARY],\n");
        }

        if (uniqueConstraint.size() > 0) {
            for (String index : uniqueConstraint.keySet()){
                queryBuilder.
                        append("CONSTRAINT ").
                        append(enCap("UK_"+modelClass.getSimpleName()+"_"+index)).
                        append(" UNIQUE NONCLUSTERED (\n");

                for (String col :  uniqueConstraint.get(index))
                    queryBuilder.append(col).append(" ASC, ");

                queryBuilder.
                        deleteCharAt(queryBuilder.lastIndexOf(",")).
                        append("\n) WITH (\n").
                        append("PAD_INDEX = OFF, ").append("STATISTICS_NORECOMPUTE = OFF, ")
                        .append("IGNORE_DUP_KEY = OFF, ").append("ALLOW_ROW_LOCKS = ON, ")
                        .append("ALLOW_PAGE_LOCKS = ON").append("\n) ON [PRIMARY],\n");

            }
        }

        queryBuilder.
                deleteCharAt(queryBuilder.lastIndexOf(",")).
                append(") ON [PRIMARY] \nGO\n");

        return queryBuilder.toString();
    }

    public static String generateSqlFromModels(Class<?>[] models) {
        HashSet<Class<?>> tableSet = new HashSet<>();
        SqlGenerator generator = new SqlGenerator();
        StringBuilder sb = new StringBuilder();
        for (int i=0; i < models.length; i++) {
            Class<?> model = models[i];
            if (!tableSet.contains(model)) {
                sb.append(
                    generator.generateSqlCreateTable(
                            model,
                            true,
                            i==0
                    )
                );
                sb.append("\n\n\n\n");
                tableSet.add(model);
            }
        }
        return sb.toString();
    }

}
