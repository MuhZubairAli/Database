package pk.gov.pbs.database.exceptions;

import android.database.Cursor;

public class ColumnNotFound extends NoSuchFieldError{
    Cursor cursor;
    public ColumnNotFound() {
    }

    public ColumnNotFound(String s) {
        super(s);
    }

    public ColumnNotFound(String s, Cursor cursor) {
        super(s);
        this.cursor = cursor;
    }
}
