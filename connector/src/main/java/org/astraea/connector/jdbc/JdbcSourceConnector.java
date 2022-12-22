package org.astraea.connector.jdbc;

import java.util.List;
import org.astraea.common.Configuration;
import org.astraea.connector.Definition;
import org.astraea.connector.MetadataStorage;
import org.astraea.connector.SourceConnector;
import org.astraea.connector.SourceTask;

public class JdbcSourceConnector extends SourceConnector {

  @Override
  protected void init(Configuration configuration, MetadataStorage storage) {

  }

  @Override
  protected Class<? extends SourceTask> task() {
    return null;
  }

  @Override
  protected List<Configuration> takeConfiguration(int maxTasks) {
    return null;
  }

  @Override
  protected List<Definition> definitions() {
    return null;
  }
}
