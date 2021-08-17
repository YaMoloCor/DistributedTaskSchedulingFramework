package org.example.job.core.biz;

import org.example.job.core.biz.model.HandleCallbackParam;
import org.example.job.core.biz.model.RegistryParam;
import org.example.job.core.biz.model.ReturnT;

import java.util.List;

public interface AdminBiz {
    // ---------------------- registry ----------------------

    /**
     * registry
     *
     * @param registryParam
     * @return
     */
    ReturnT<String> registry(RegistryParam registryParam);

    /**
     * registryRemove
     * @param registryParam
     * @return
     */
    ReturnT<String> registryRemove(RegistryParam registryParam);

    /**
     * callback
     * @param callbackParamList
     * @return
     */
    ReturnT<String> callback(List<HandleCallbackParam> callbackParamList);
}
