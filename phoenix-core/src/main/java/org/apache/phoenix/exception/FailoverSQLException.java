package org.apache.phoenix.exception;

import java.sql.SQLException;

/**
 * A subclass of SQException thrown in case of failover errors.
 */
public class FailoverSQLException extends SQLException {
  private final String haGroupInfo;

  public FailoverSQLException(String reason, String haGroupInfo, Throwable cause) {
    super("reason=" + reason + ", haGroupInfo=" + haGroupInfo, cause);
    this.haGroupInfo = haGroupInfo;
  }

  public String getFailoverGroup() {
    return haGroupInfo;
  }
}
