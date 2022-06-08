package com.janezt.yugong;

import org.junit.Test;

/**
 * @program： Batch-Utils
 * @description：
 * @author： 愚工
 * @date： 2022/5/10
 * @company：深圳市减字科技有限公司
 */
public class BatchTaskTest
{

    @Test
    public void test() throws Exception
    {
        //定义一个任务处理器
        TestTaskProcessor processor = new TestTaskProcessor();
        //定义一个任务调度器
        TaskDispatcher<Integer, TestTask> taskDispatcher = TaskDispatchers.createBatchingTaskDispatcher("batchTasks", 1000000, 500, 200, 500L, 500L, 500L, processor);
        for (int i = 0; i < 1000000; i++)
        {
            long time = System.currentTimeMillis() + 500 * 1000;
            taskDispatcher.process(i, new TestTask(i + 1 + ""), time);
        }
        try
        {
            Thread.sleep(100000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
