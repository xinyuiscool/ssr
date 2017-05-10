package org.apache.samza.sql.system;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;


/**
 * Created by xiliu on 5/10/17.
 */
public class DummySystemAdmin implements SystemAdmin {
  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    return offsets.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, null));
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    // TODO: is this the right way to bridge partitioning scheme with Beam?
    return streamNames.stream()
        .collect(Collectors.toMap(
            Function.<String>identity(),
            streamName -> new SystemStreamMetadata(streamName,
                Collections.singletonMap(new Partition(0),
                    new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, null)))));
  }

  @Override
  public void createChangelogStream(String streamName, int numOfPartitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void validateChangelogStream(String streamName, int numOfPartitions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createCoordinatorStream(String streamName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    if (offset1 == null) {
      return offset2 == null ? 0 : -1;
    } else if (offset2 == null) {
      return 1;
    }
    return offset1.compareTo(offset2);
  }
}

