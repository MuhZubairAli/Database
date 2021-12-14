package pk.gov.pbs.database.exceptions;

public class UnsupportedDataType extends Exception{
    private Class<?> mClass;
    public UnsupportedDataType(String message, Class<?> mClass) {
        super(message);
        this.mClass = mClass;
    }
    public UnsupportedDataType(Class<?> mClass) {
        super("Data type not supported for this operation");
        this.mClass = mClass;
    }
}
