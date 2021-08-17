package org.example.job.admin.route.strategy;

import org.example.job.admin.route.ExecutorRouter;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.biz.model.TriggerParam;

import java.util.List;

public class ExecutorRouteFailover extends ExecutorRouter {
    @Override
    public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
        return null;
    }
}
