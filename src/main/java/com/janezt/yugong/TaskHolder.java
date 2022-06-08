package com.janezt.yugong;

/**
 * @program： Batch-Utils
 * @description：任务容器对象
 * @author： 愚工
 * @date： 2022/5/5
 * @company：深圳市减字科技有限公司
 */
public class TaskHolder<ID, T>
{
    /**
     * 任务id
     */
    private final ID id;
    /**
     * 任务对象
     */
    private final T task;
    /**
     * 任务过期时间
     */
    private final long expiryTime;
    /**
     * 任务提交时间
     */
    private final long submitTimestamp;

    TaskHolder(ID id, T task, long expiryTime)
    {
        this.id = id;
        this.expiryTime = expiryTime;
        this.task = task;
        this.submitTimestamp = System.currentTimeMillis();
    }

    public ID getId()
    {
        return id;
    }

    public T getTask()
    {
        return task;
    }

    public long getExpiryTime()
    {
        return expiryTime;
    }

    public long getSubmitTimestamp()
    {
        return submitTimestamp;
    }
}
