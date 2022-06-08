package com.janezt.yugong;

/**
 * @program： Batch-Utils
 * @description：任务调度器组件
 * @author： 愚工
 * @date： 2022/5/5
 * @company：深圳市减字科技有限公司
 */
public class TaskDispatchers
{
    /**
     * 创建任务调度器
     *
     * @param id                     任务id
     * @param maxBufferSize          最大任务数量
     * @param workloadSize           批量包最大任务数量
     * @param workerCount            线程数量
     * @param maxBatchingDelay       最大批处理延迟(批处理打包间隔)
     * @param congestionRetryDelayMs 运行阻塞重试延迟时间(ms)
     * @param networkFailureRetryMs  运行异常重试延迟时间(ms)
     * @param taskProcessor          任务处理器
     * @param <ID>  任务id类型
     * @param <T>   任务实例类型
     * @return
     */
    public static <ID, T> TaskDispatcher<ID, T> createBatchingTaskDispatcher(String id,
                                                                             int maxBufferSize,
                                                                             int workloadSize,
                                                                             int workerCount,
                                                                             long maxBatchingDelay,
                                                                             long congestionRetryDelayMs,
                                                                             long networkFailureRetryMs,
                                                                             TaskProcessor<T> taskProcessor)
    {
        //1.创建一个AcceptorExecutor
        final AcceptorExecutor<ID, T> acceptorExecutor = new AcceptorExecutor<>(
                id, maxBufferSize, workloadSize, maxBatchingDelay, congestionRetryDelayMs, networkFailureRetryMs
        );
        //2.创建一个TaskExecutors
        final TaskExecutors<ID, T> taskExecutor = TaskExecutors.batchExecutors(id, workerCount, taskProcessor, acceptorExecutor);
        //3.创建一个 TaskDispatcher
        return new TaskDispatcher<ID, T>()
        {
            @Override
            public void process(ID id, T task, long expiryTime)
            {
                acceptorExecutor.process(id, task, expiryTime);
            }

            @Override
            public void shutdown()
            {
                acceptorExecutor.shutdown();
                taskExecutor.shutdown();
            }
        };
    }
}
