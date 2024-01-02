package pk.gov.pbs.database.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SqlPrimaryKey {
    public boolean autogenerate() default false;
    public int seed() default 1;
    public int increment() default 1;
}
