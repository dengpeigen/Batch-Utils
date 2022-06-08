package com.janezt.yugong;

/**
 * @program： Batch-Utils
 * @description：任务处理结果枚举 成功 阻塞 运行错误 永久错误
 * @author： 愚工
 * @date： 2022/5/6
 * @company：深圳市减字科技有限公司
 */
public enum ProcessingResult
{
    Success,
    Congestion,
    TransientError,
    PermanentError
}
