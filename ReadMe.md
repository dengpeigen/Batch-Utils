# Batch-Utils(批处理工具包)

### 1.使用方法

**1.1 引入maven依赖**

```
<dependency>
    <groupId>com.janezt.yugong</groupId>
    <artifactId>Batch-Utils</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

**1.2 定义一个任务数据对象，用来承载任务信息**

```
/**
 * @program： Batch-Utils
 * @description：任务实例对象
 * @author： 愚工
 * @date： 2022/5/10
 * @company：深圳市减字科技有限公司
 */
public class TestTask
{
    public TestTask(String name){
        this.name = name;
    }
    private String name;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }
}
```

**1.3 定义一个任务处理器的实现，实现TaskProcessor<TestTask>**

```java
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
```

**1.4 初始化任务处理器**

**1.5 初始化任务调度器**

核心参数:

```java
/**
 * 创建任务调度器
 *
 * @param id                     任务id
 * @param maxBufferSize          批量包最大任务数量
 * @param workloadSize           任务加载大小
 * @param workerCount            线程数量
 * @param maxBatchingDelay       最大批处理延迟(批处理打包间隔)
 * @param congestionRetryDelayMs 运行阻塞重试延迟时间(ms)
 * @param networkFailureRetryMs  运行异常重试延迟时间(ms)
 * @param taskProcessor          任务处理器
 * @param <ID>  任务id类型
 * @param <T>   任务实例类型
 * @return
 */
```

**1.6 提交任务到任务调度器**

```java
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
```

### 2.核心原理（三层队列+信号量）

![image-20220608093720554](https://janezt-agentoss.oss-cn-beijing.aliyuncs.com/batch-utils.png)

### 3.核心类

##### 3.1 TaskHolder

任务容器对象

用于承载任务对象信息，额外会记录任务的id、过期时间、提交时间。

##### 3.2 AcceptorExecutor

任务接收处理器

三层队列容器、接收任务、批次打包、流量控制。

##### 3.3 TaskDispatcher

任务调度器模板接口

定义任务处理方法、组件停止方法

##### 3.4 TaskDispatchers

任务调度器组件

定义任务调度器的创建方法

##### 3.5 TaskExecutors

任务执行组件

创建工作线程、获取批处理队列数据、执行具体任务、失败任务重试、记录任务执行结果

##### 3.6 TrafficShaper

异常记录器

定义异常配置、记录异常数据

### 4.代码地址

4.1git地址   http://192.168.99.41:28088/root/batch-utils

4.2maven私服地址   http://120.79.159.177:8582/

4.3maven依赖

```
<dependency>
  <groupId>com.janezt.yugong</groupId>
  <artifactId>Batch-Utils</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

