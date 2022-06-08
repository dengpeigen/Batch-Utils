package com.janezt.yugong;

import java.util.List;

/**
 * @program： Batch-Utils
 * @description： 任务处理器模板  目前只做了批量处理     这里可以做单任务、定时任务 的拓展
 * @author： 愚工
 * @date： 2022/5/5
 * @company：深圳市减字科技有限公司
 */
public interface TaskProcessor<T>
{

    /**
     * 批量处理任务
     *
     * @param tasks
     * @return
     */
    ProcessingResult process(List<T> tasks);
}