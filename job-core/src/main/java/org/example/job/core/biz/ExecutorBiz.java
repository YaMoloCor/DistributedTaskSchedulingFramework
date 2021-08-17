package org.example.job.core.biz;

import org.example.job.core.biz.model.*;

public interface ExecutorBiz {
    /**
     * run
     * @param triggerParam
     * @return
     */
    ReturnT<String> run(TriggerParam triggerParam);

    /**
     *
     * @param logParam
     * @return
     */
    ReturnT<LogResult> log(LogParam logParam);

    /**
     *
     * @param killParam
     * @return
     */
    ReturnT<String> kill(KillParam killParam);
}
