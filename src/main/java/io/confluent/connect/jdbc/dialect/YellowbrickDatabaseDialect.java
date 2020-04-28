/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;

public class YellowbrickDatabaseDialect extends PostgreSqlDatabaseDialect {
  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public YellowbrickDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(YellowbrickDatabaseDialect.class.getSimpleName(), "yellowbrick");
    }

    public DatabaseDialect create(AbstractConfig config) {
      return new YellowbrickDatabaseDialect(config);
    }
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    switch (field.schemaType()) {
      case STRING:
        return "VARCHAR(MAX)";
      case BYTES:
        throw new ConnectException(String.format(
          "%s (%s) type doesn't have a mapping to the SQL database column type",
          field.schemaName(),
          field.schemaType()
        ));
      default:
        return super.getSqlType(field);
    }
  }
}
