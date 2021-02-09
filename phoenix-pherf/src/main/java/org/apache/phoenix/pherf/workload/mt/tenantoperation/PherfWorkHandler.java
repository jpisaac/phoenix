package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.workload.mt.OperationStats;

import java.util.List;

public interface PherfWorkHandler<T> extends WorkHandler<T> {
    List<ResultValue<OperationStats>> getResults();
}
