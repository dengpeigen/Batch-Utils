package com.janezt.yugong;

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
