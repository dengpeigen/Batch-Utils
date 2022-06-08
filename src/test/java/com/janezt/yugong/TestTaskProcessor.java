package com.janezt.yugong;

import java.util.List;

/**
 * @program： Batch-Utils
 * @description：任务处理器实现
 * @author： 愚工
 * @date： 2022/5/10
 * @company：深圳市减字科技有限公司
 */
public class TestTaskProcessor implements TaskProcessor<TestTask>
{

    @Override
    public ProcessingResult process(List<TestTask> tasks)
    {
        for (TestTask task : tasks) {
            System.out.print(task.getName()+",");

        }
        System.out.println();
        return ProcessingResult.Success;
    }
}
