package org.astraea.connector.jdbc.source;

import java.util.Collection;
import org.astraea.common.Configuration;
import org.astraea.common.producer.Record;
import org.astraea.connector.SourceTask;

public class JdbcSourceTask implements SourceTask {

  @Override
  protected void init(Configuration configuration) {

  }

  @Override
  protected Collection<Record<byte[], byte[]>> take() throws InterruptedException {
    return null;
  }
}
