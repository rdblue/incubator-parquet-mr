package parquet.avro;

import parquet.ParquetRuntimeException;

/**
 * Thrown if there is a problem converting an Avro type.
 */
public class AvroTypeException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public AvroTypeException() {
  }

  public AvroTypeException(String message, Throwable cause) {
    super(message, cause);
  }

  public AvroTypeException(String message) {
    super(message);
  }

  public AvroTypeException(Throwable cause) {
    super(cause);
  }
}