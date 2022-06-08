package com.janezt.yugong;

/**
 * @program： Batch-Utils
 * @description：异常记录器
 * @author： 愚工
 * @date： 2022/5/6
 * @company：深圳市减字科技有限公司
 */
public class TrafficShaper
{

    /**
     * 延迟上限配置  写死为 30s
     */
    private static final long MAX_DELAY = 30 * 1000;

    /**
     * 运行阻塞重试延迟时间(ms)
     */
    private final long congestionRetryDelayMs;
    /**
     * 运行异常重试延迟时间(ms)
     */
    private final long networkFailureRetryMs;

    /**
     * 上次运行阻塞指针
     */

    private volatile long lastCongestionError;
    /**
     * 上次运行异常指针
     */
    private volatile long lastNetworkFailure;

    TrafficShaper(long congestionRetryDelayMs, long networkFailureRetryMs) {
        /**
         * 初始化延迟配置
         */
        this.congestionRetryDelayMs = Math.min(MAX_DELAY, congestionRetryDelayMs);
        this.networkFailureRetryMs = Math.min(MAX_DELAY, networkFailureRetryMs);
    }

    /**
     * 异常记录
     * @param processingResult
     */
    void registerFailure(ProcessingResult processingResult) {
        if (processingResult == ProcessingResult.Congestion) {
            lastCongestionError = System.currentTimeMillis();
        } else if (processingResult == ProcessingResult.TransientError) {
            lastNetworkFailure = System.currentTimeMillis();
        }
    }

    /**
     * 执行延迟
     * @return
     */
    long transmissionDelay() {
        //如果没有发生运行阻塞和运行异常的话  直接返回0 不需要延迟
        if (lastCongestionError == -1 && lastNetworkFailure == -1) {
            return 0;
        }

        long now = System.currentTimeMillis();
        //如果发生运行阻塞的话
        if (lastCongestionError != -1) {
            //记录发生运行阻塞的延迟时间
            long congestionDelay = now - lastCongestionError;
            //判断发生运行阻塞的延迟时间是否小于阈值 如果小于的话 就返回差值
            if (congestionDelay >= 0 && congestionDelay < congestionRetryDelayMs) {
                return congestionRetryDelayMs - congestionDelay;
            }
            //重置阻塞记录
            lastCongestionError = -1;
        }
        //如果发生运行异常的话
        if (lastNetworkFailure != -1) {
            //记录发生运行异常的延迟时间
            long failureDelay = now - lastNetworkFailure;
            //判断发生运行异常的延迟时间是否小于阈值 如果小于的话 就返回差值
            if (failureDelay >= 0 && failureDelay < networkFailureRetryMs) {
                return networkFailureRetryMs - failureDelay;
            }
            //重置运行异常记录
            lastNetworkFailure = -1;
        }
        return 0;
    }
}
