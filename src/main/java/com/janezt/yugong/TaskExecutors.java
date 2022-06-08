package com.janezt.yugong;


import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @program： Batch-Utils
 * @description：任务执行组件
 * @author： 愚工
 * @date： 2022/5/5
 * @company：深圳市减字科技有限公司
 */
public class TaskExecutors<ID, T>
{

    /**
     * 是否停止  使用原子类做线程同步
     */
    private final AtomicBoolean isShutdown;
    /**
     * 线程集合
     */
    private final List<Thread> workerThreads;

    TaskExecutors(WorkerRunnableFactory<ID, T> workerRunnableFactory, int workerCount, AtomicBoolean isShutdown)
    {
        //初始化
        this.isShutdown = isShutdown;
        this.workerThreads = new ArrayList<>();

        //定义线程分组  方便问题定位
        ThreadGroup threadGroup = new ThreadGroup("batchTheadGroup");
        //根据线程数量 初始化线程，并且放入到线程集合中
        for (int i = 0; i < workerCount; i++)
        {
            WorkerRunnable<ID, T> runnable = workerRunnableFactory.create(i);
            Thread workerThread = new Thread(threadGroup, runnable, runnable.getWorkerName());
            workerThreads.add(workerThread);
            //设置为守护线程
            workerThread.setDaemon(true);
            workerThread.start();
        }
    }

    /**
     * 停止组件工作
     */
    void shutdown()
    {
        if (isShutdown.compareAndSet(false, true))
        {
            //中断当前容器中的所有线程
            for (Thread workerThread : workerThreads)
            {
                workerThread.interrupt();
            }
        }
    }


    /**
     * 初始化任务执行组件
     * @param name  任务处理器名称
     * @param workerCount   工作线程数量
     * @param processor  关联任务处理器
     * @param acceptorExecutor  关联任务接收处理器
     * @param <ID>
     * @param <T>
     * @return
     */
    static <ID, T> TaskExecutors<ID, T> batchExecutors(final String name,
                                                       int workerCount,
                                                       final TaskProcessor<T> processor,
                                                       final AcceptorExecutor<ID, T> acceptorExecutor)
    {
        final AtomicBoolean isShutdown = new AtomicBoolean();
        return new TaskExecutors<>(idx -> new BatchWorkerRunnable<>("TaskBatchingWorker-" + name + '-' + idx, isShutdown, processor, acceptorExecutor), workerCount, isShutdown);
    }

    /**
     * 定义任务现线程工厂
     * @param <ID>
     * @param <T>
     */
    interface WorkerRunnableFactory<ID, T>
    {
        /**
         * 创建线程
         * @param idx
         * @return
         */
        WorkerRunnable<ID, T> create(int idx);
    }

    /**
     * 定义模板方法   工作线程的定义   目前只做了批处理工作线程  之后可以拓展单任务、定时任务等方式
     * @param <ID>
     * @param <T>
     */
    abstract static class WorkerRunnable<ID, T> implements Runnable
    {
        /**
         * 线程名称
         */
        final String workerName;
        /**
         * 线程是否停止
         */
        final AtomicBoolean isShutdown;
        /**
         * 关联任务处理器
         */
        final TaskProcessor<T> processor;
        /**
         * 关联任务接收处理器
         */
        final AcceptorExecutor<ID, T> taskDispatcher;

        WorkerRunnable(String workerName,
                       AtomicBoolean isShutdown,
                       TaskProcessor<T> processor,
                       AcceptorExecutor<ID, T> taskDispatcher)
        {
            this.workerName = workerName;
            this.isShutdown = isShutdown;
            this.processor = processor;
            this.taskDispatcher = taskDispatcher;
        }

        String getWorkerName()
        {
            return workerName;
        }
    }

    /**
     * 批处理工作线程
     * @param <ID>
     * @param <T>
     */
    static class BatchWorkerRunnable<ID, T> extends WorkerRunnable<ID, T>
    {

        BatchWorkerRunnable(String workerName,
                            AtomicBoolean isShutdown,
                            TaskProcessor<T> processor,
                            AcceptorExecutor<ID, T> acceptorExecutor)
        {
            super(workerName, isShutdown, processor, acceptorExecutor);
        }

        Log log = LogFactory.get();

        @Override
        public void run()
        {
            try
            {
                while (!isShutdown.get())
                {
                    //获取任务容器
                    List<TaskHolder<ID, T>> holders = getWork();
                    //通过容器获取任务实例
                    List<T> tasks = getTasksOf(holders);
                    //批处理任务
                    ProcessingResult result = processor.process(tasks);
                    switch (result)
                    {
                        case Success:
                            break;
                        case Congestion:
                        case TransientError:
                            //如果是非永久性失败的任务的话 进行任务重试
                            taskDispatcher.reprocess(holders, result);
                            break;
                        case PermanentError:
                            //失败次数达到阈值 直接略过任务  记录日志
                            log.info("失败次数达到阈值,转化成永久失败",tasks);

                    }
                }
            }
            catch (InterruptedException e)
            {
                // 中断线程  之后可以考虑做功能升级
            }
            catch (Throwable e)
            {
                //异常记录
                log.error("执行任务错误",e);
            }
        }

        /**
         * 获取任务容器集合
         * @return
         * @throws InterruptedException
         */
        private List<TaskHolder<ID, T>> getWork() throws InterruptedException
        {
            //释放信号量，获取第三层批处理队列
            BlockingQueue<List<TaskHolder<ID, T>>> workQueue = taskDispatcher.requestWorkItems();
            List<TaskHolder<ID, T>> result;
            do
            {
                //从批处理队列中获取任务
                result = workQueue.poll(1, TimeUnit.SECONDS);
                //当组件没有停止 并且 也拿不到任何任务数据的时候 跳出循环
            } while (!isShutdown.get() && result == null);
            return (result == null) ? new ArrayList<>() : result;
        }

        /**
         * 从任务容器集合中获取任务对象实例集合
         * @param holders
         * @return
         */
        private List<T> getTasksOf(List<TaskHolder<ID, T>> holders)
        {
            List<T> tasks = new ArrayList<>(holders.size());
            for (TaskHolder<ID, T> holder : holders)
            {
                tasks.add(holder.getTask());
            }
            return tasks;
        }
    }
}
