package com.janezt.yugong;

/**
 * @program： Batch-Utils
 * @description：任务调度器
 * @author： 愚工
 * @date： 2022/5/5
 * @company：深圳市减字科技有限公司
 */
public interface TaskDispatcher<ID, T>
{

    /**
     * 处理任务
     *
     * @param id
     * @param task
     * @param expiryTime
     */
    void process(ID id, T task, long expiryTime);

    /**
     * 结束
     */
    void shutdown();
}
