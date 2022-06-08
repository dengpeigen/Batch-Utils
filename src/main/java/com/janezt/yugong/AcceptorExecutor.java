package com.janezt.yugong;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @program： Batch-Utils
 * @description： 任务接收处理器
 * @author： 愚工
 * @date： 2022/5/5
 * @company：深圳市减字科技有限公司
 */
public class AcceptorExecutor<ID, T>
{

    Log log = LogFactory.get();

    private final String id;
    /**
     * 最大任务数量
     */
    private final int maxBufferSize;
    /**
     * 批量包 最大任务数量
     */
    private final int maxBatchingSize;
    /**
     * 任务批处理间隔延迟时间
     */
    private final long maxBatchingDelay;
    /**
     * 组件是否停止标志
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * 任务队列  第一层队列
     */
    private final BlockingQueue<TaskHolder<ID, T>> acceptorQueue = new LinkedBlockingQueue<>();
    /**
     * 任务重试队列
     */
    private final BlockingDeque<TaskHolder<ID, T>> reprocessQueue = new LinkedBlockingDeque<>();
    /**
     * 接任务线程
     */
    private final Thread acceptorThread;
    /**
     * 挂起任务map
     */
    private final Map<ID, TaskHolder<ID, T>> pendingTasks = new HashMap<>();
    /**
     * 打包任务队列 第二层队列
     */
    private final Deque<ID> processingOrder = new LinkedList<>();
    /**
     * 批处理流量控制器  信号量
     */
    private final Semaphore batchWorkRequests = new Semaphore(0);
    /**
     * 批处理任务队列  第三层队列
     */
    private final BlockingQueue<List<TaskHolder<ID, T>>> batchWorkQueue = new LinkedBlockingQueue<>();

    /**
     * 流量异常记录器
     */
    private final TrafficShaper trafficShaper;

    AcceptorExecutor(String id,
                     int maxBufferSize,
                     int maxBatchingSize,
                     long maxBatchingDelay,
                     long congestionRetryDelayMs,
                     long networkFailureRetryMs)
    {
        this.id = id;
        //初始化参数
        this.maxBufferSize = maxBufferSize;
        this.maxBatchingSize = maxBatchingSize;
        this.maxBatchingDelay = maxBatchingDelay;
        //设置异常记录器相关的参数
        this.trafficShaper = new TrafficShaper(congestionRetryDelayMs, networkFailureRetryMs);

        //以Daemon的方式运行一个线程
        //具体逻辑在AcceptorRunner中实现
        ThreadGroup threadGroup = new ThreadGroup("batchTaskExecutors");
        this.acceptorThread = new Thread(threadGroup, new AcceptorRunner(), "TaskAcceptor-" + id);
        this.acceptorThread.setDaemon(true);
        this.acceptorThread.start();
    }

    /**
     * 处理任务
     *
     * @param id
     * @param task
     * @param expiryTime
     */
    void process(ID id, T task, long expiryTime)
    {
        /**
         * 将任务丢进第一层队列中
         */
        acceptorQueue.add(new TaskHolder<ID, T>(id, task, expiryTime));
    }

    /**
     * 任务重试
     *
     * @param holders
     * @param processingResult
     */
    void reprocess(List<TaskHolder<ID, T>> holders, ProcessingResult processingResult)
    {
        /**
         * 将失败的任务丢进重试队列中
         */
        reprocessQueue.addAll(holders);
        /**
         * 记录异常数据
         */
        trafficShaper.registerFailure(processingResult);
    }

    /**
     * 任务重试
     *
     * @param taskHolder
     * @param processingResult
     */
    void reprocess(TaskHolder<ID, T> taskHolder, ProcessingResult processingResult)
    {
        /**
         * 将失败的任务丢进重试队列中
         */
        reprocessQueue.add(taskHolder);
        /**
         * 记录异常数据
         */
        trafficShaper.registerFailure(processingResult);
    }


    /**
     * 获取批处理队列
     *
     * @return
     */
    BlockingQueue<List<TaskHolder<ID, T>>> requestWorkItems()
    {
        //释放信号量控制
        batchWorkRequests.release();
        return batchWorkQueue;
    }

    /**
     * 停止组件运行
     */
    void shutdown()
    {
        if (isShutdown.compareAndSet(false, true))
        {
            //中断接收任务的线程
            acceptorThread.interrupt();
        }
    }


    class AcceptorRunner implements Runnable
    {
        @Override
        public void run()
        {
            long scheduleTime = 0;
            //设置一个死循环
            while (!isShutdown.get())
            {
                try
                {
                    //处理输入队列
                    drainInputQueues();

                    //总数 = 处理中的大小
                    int totalItems = processingOrder.size();

                    //重置scheduleTime
                    long now = System.currentTimeMillis();
                    if (scheduleTime < now)
                    {
                        scheduleTime = now + trafficShaper.transmissionDelay();
                    }
                    //分配任务
                    if (scheduleTime <= now)
                    {
                        assignBatchWork();
                    }

                    //如果当前没有任务可以处理的时候，sleep一会儿，避免太过紧密的循环 拖累系统
                    if (totalItems == processingOrder.size())
                    {
                        Thread.sleep(10);
                    }
                }
                catch (InterruptedException ex)
                {
                    // Ignore
                }
                catch (Throwable e)
                {
                    log.error("接受程序线程错误",e);
                }
            }
        }

        /**
         * 是否已满
         *
         * @return
         */
        private boolean isFull()
        {
            return pendingTasks.size() >= maxBufferSize;
        }

        /**
         * 安排输入队列
         *
         * @throws InterruptedException
         */
        private void drainInputQueues() throws InterruptedException
        {
            do
            {
                drainReprocessQueue();
                drainAcceptorQueue();

                if (isShutdown.get())
                {
                    break;
                }
                // 如果所有队列都为空，在接受方队列上阻塞一段时间
                if (reprocessQueue.isEmpty() && acceptorQueue.isEmpty() && pendingTasks.isEmpty())
                {
                    TaskHolder<ID, T> taskHolder = acceptorQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (taskHolder != null)
                    {
                        appendTaskHolder(taskHolder);
                    }
                }
            } while (!reprocessQueue.isEmpty() || !acceptorQueue.isEmpty() || pendingTasks.isEmpty());
        }

        /**
         * 安排接收队列
         */
        private void drainAcceptorQueue()
        {
            //如果acceptorQueue 不为空则返回任务取出放到pendingTasks中
            while (!acceptorQueue.isEmpty())
            {
                appendTaskHolder(acceptorQueue.poll());
            }
        }

        /**
         * 安排重试队列
         */
        private void drainReprocessQueue()
        {
            long now = System.currentTimeMillis();
            //reprocessQueue 不为空 且 pendingTasks还没满
            while (!reprocessQueue.isEmpty() && !isFull())
            {
                TaskHolder<ID, T> taskHolder = reprocessQueue.pollLast();
                ID id = taskHolder.getId();
                //如果过期了
                if (taskHolder.getExpiryTime() <= now)
                {
                }
                else if (pendingTasks.containsKey(id))
                {
                    //如果处理中的任务包含该任务
                }
                else
                {
                    //添加任务到pendingTasks,添加到processingOrder的头部
                    pendingTasks.put(id, taskHolder);
                    processingOrder.addFirst(id);
                }
            }
            //如果满了的话,清空reprocessQueue
            if (isFull())
            {
                reprocessQueue.clear();
            }
        }

        /**
         * 追加taskHolder
         *
         * @param taskHolder
         */
        private void appendTaskHolder(TaskHolder<ID, T> taskHolder)
        {
            //如果待处理的任务满了，则从processingOrder 中拿出一个任务，然后从待处理任务中 移除。队列溢出的值+1
            if (isFull())
            {
                pendingTasks.remove(processingOrder.poll());
            }
            //然后放入待处理的Map中
            TaskHolder<ID, T> previousTask = pendingTasks.put(taskHolder.getId(), taskHolder);
            //如果没有放入成功，则把其加入 processingOrder中
            if (previousTask == null)
            {
                processingOrder.add(taskHolder.getId());
            }
            else
            {
                //加入成功的话 可以做其他的扩展
            }
        }

        /**
         * 分配批量任务
         */
        void assignBatchWork()
        {
            //当有足够的任务进入的时候
            if (hasEnoughTasksForNextBatch())
            {
                //使用信号量控制进入
                if (batchWorkRequests.tryAcquire(1))
                {
                    long now = System.currentTimeMillis();
                    //取processingOrder和maxBatchingSize中的最小值。如果任务过多 按照maxBatchingSize 分队列
                    int len = Math.min(maxBatchingSize, processingOrder.size());
                    List<TaskHolder<ID, T>> holders = new ArrayList<>(len);
                    //往holders中添加数据
                    while (holders.size() < len && !processingOrder.isEmpty())
                    {
                        //从processingOrder中获取数据
                        ID id = processingOrder.poll();
                        //从pendingTasks中移除
                        TaskHolder<ID, T> holder = pendingTasks.remove(id);
                        //判断是否过期，不过期加入
                        if (holder.getExpiryTime() > now)
                        {
                            holders.add(holder);
                        }
                        else
                        {
                            //过期了的话  可以做其他的扩展
                        }
                    }
                    //如果holders为空，则说明当前没有任务，释放信号
                    if (holders.isEmpty())
                    {
                        batchWorkRequests.release();
                    }
                    else
                    {
                        //否则记录一下数据，添加到batchWorkQueue中
                        batchWorkQueue.add(holders);
                    }
                }
            }
        }

        /**
         * 判断是否有足够的任务用于下一次批处理
         *
         * @return
         */
        private boolean hasEnoughTasksForNextBatch()
        {
            /**
             * 如果processingOrder为空，则返回false
             */
            if (processingOrder.isEmpty())
            {
                return false;
            }
            //如果pendingTasks大小大于maxBufferSize 返回ture
            if (pendingTasks.size() >= maxBufferSize)
            {
                return true;
            }

            //从processingOrder中取出在pendingTasks中获取
            TaskHolder<ID, T> nextHolder = pendingTasks.get(processingOrder.peek());
            //计算出下一个任务的提交时间到现在的时间间隔
            long delay = System.currentTimeMillis() - nextHolder.getSubmitTimestamp();
            //判断延迟是否大于等于默认延迟时间500ms 返回true
            return delay >= maxBatchingDelay;
        }
    }
}