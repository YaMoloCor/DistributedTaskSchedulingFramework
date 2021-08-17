package org.example.job.admin.route;

import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.biz.model.TriggerParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class ExecutorRouter {
    protected static Logger logger = LoggerFactory.getLogger(ExecutorRouter.class);

    /**
     * route 抽象方法
     * @param triggerParam
     * @param addressList
     * @return
     */
    public abstract ReturnT<String> route(TriggerParam triggerParam, List<String> addressList);
}
