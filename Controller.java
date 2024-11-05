package Controller;

import Hardware_Data.Hardware_Data;
import Mq.Sender;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Controller extends Thread{

//数据区：
    Sender sender=null;//消息发送器
    Jedis jedis=null;//redis连接
    private  String Redis_Server_IP;//部署redis的主机的ip
    private String redis_host="DESKTOP-L20KP0M";//默认我的id
    private  String Activemq_IP;
    private String Activemq_host="DESKTOP-L20KP0M";//默认沛文的主机
    private boolean isWork=false;//工作标记

    //等待任务列表(id+time)
    Set<String> car_W=new HashSet<>();//等待导航的小车序列集
    Set<String> task_N=new HashSet<>();//等待评判的任务集
    Set<String> task_T=new HashSet<>();//等待小车接取的任务集

    //允许最久等待次数
    private static final int wait_time= 30;

//方法区：

    //无参构造
    public Controller() {
        this.Redis_Server_IP=new Hardware_Data(redis_host).getIP();
        this.Activemq_IP=new Hardware_Data(Activemq_host).getIP();
    }

    //含参构造
    public Controller(String redis_host,String Activemq_host) {
        this.Redis_Server_IP=new Hardware_Data(redis_host).getIP();
        this.Activemq_IP=new Hardware_Data(Activemq_host).getIP();
    }

    //控制器开始控制
    public void startControl()
    {
        System.out.println("控制器开始工作...");
        //初始化消息发送器
        this.sender=new Sender(Activemq_IP,61616,"admin","admin");
        //启动发送器
        this.sender.startWork();
        //连接黑板
        jedis=new Jedis(Redis_Server_IP,6379);
        jedis.auth("group8");
        //打开标记
        isWork=true;
        //启动控制
        this.Run();
    }

    //线程方法控制
    public void Run()
    {
        super.run();
        long lastUpdate=System.currentTimeMillis();//当前系统时间
        int fps=4;
        while(isWork)
        {
            long interval=1000/fps;
            long curr=System.currentTimeMillis();
            long _time=curr-lastUpdate;

            if(_time<interval)
            {
                //不到时间，休眠
                try
                {
                 Thread.sleep(1);
                }catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            else//到时间，进行事务处理
            {
                // 更新状态
                lastUpdate = curr;

                //0:检查探索度
                over_explore();

                //刷新等待列表
                upAllSet();

                //2:处理车
                car_arrive();

                //3:处理任务
                analysis_task();
            }

        }
    }

//辅助函数:
    //获取一个新的空闲分析器的编号
    public String get_a_free_analysis()
    {
        Set<String> analysisState=jedis.smembers("analysis_state");
        for(String n:analysisState)
        {
            if(n.endsWith("W"))
            {
                return n.split(",")[0];
            }
        }
        return null;//找不到则返回null
    }

    //获取一个空闲导航器的编号
    public String get_a_free_navigate()
    {
        Set<String> navigates=jedis.smembers("navigates");
        for(String n:navigates)
        {
            if(n.endsWith("W"))
            {
                return n.split(",")[0];
            }
        }
        return null;//找不到则返回null
    }

    //查找集合中的元素，第三个标志位标记是否找到就删
    public boolean FindById(String id,Set<String> set,boolean isDelete)
    {
        Iterator<String> iterator=set.iterator();
        while(iterator.hasNext())
        {
            String temp=iterator.next();
            if(temp.startsWith(id))
            {   //找到
                if(isDelete)
                {
                    iterator.remove();
                }
                return true;
            }
        }
        //没找到
        return false;
    }

    //刷新所有任务列表
    public void upAllSet()
    {
        upASet(car_W);
        upASet(task_N);
        upASet(task_T);
    }

    //刷新指定任务列表
    public void upASet(Set<String> key) {
        Set<String> toAdd = new HashSet<>();
        Iterator<String> a = key.iterator();
        while(a.hasNext()) {
            String temp0 = a.next();
            a.remove(); // 删除元素
            String[] temp1 = temp0.split(",");
            int x = Integer.parseInt(temp1[1]) - 1;
            if(x > 0) {
                toAdd.add(temp1[0] + "," + x); // 收集需要添加的元素
            }
        }
        key.addAll(toAdd); // 应用收集的更改
    }

    //检查探索进度是否满
    public void over_explore()
    {
        int explore=Integer.parseInt(jedis.get("explore"));
        int mapSp=Integer.parseInt(jedis.get("map_sp"));
        if(explore==mapSp*mapSp)//探索满
        {
            System.out.println("探索结束！");
            //通知所有小车存档
            Set<String> cars=jedis.smembers("cars");
            //遍历所有小车，给他们发任务
            for(String s:cars)
            {
                sendMessage("Car"+s.split(",")[0],"save");
            }
            //等待所有小车全部清空
            while(jedis.smembers("cars").size()>0)
            {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            //重置State
            jedis.set("State","null");
            //关闭控制
            this.endControl();
        }
    }

    //处理任务新状态
    public void analysis_task()
    {
        //获取任务列表
        Iterator<String> tasks=jedis.smembers("task").iterator();
        //遍历任务列表
        while (tasks.hasNext()) {
            String temp_task = tasks.next();
            String task[]=temp_task.split(",");

            if(task[2].equals("N"))
            {
                //任务未评定，调度评估器评定
                if(!FindById(task[0],task_N,false))
                {
                    //新的未评任务,发布任务并等待
                    String free_analysis=get_a_free_analysis();
                    if(free_analysis!=null)
                    {
                        sendMessage("Analysis"+free_analysis,"analysis,"+task[0]);
                        task_N.add(task[0]+","+wait_time);
                    }
                }
            }
            else if(task[2].equals("T"))
            {
                //任务评定成功，通知小车并等待
                //1:刚成功的任务，N表有，而T表没有--删N表，加T表，通知小车
                if(!FindById(task[0],task_T,false))
                {
                    FindById(task[0],task_N,true);
                    task_T.add(task[0]+","+wait_time);
                    sendMessage("Car"+task[0],"fetch");
                }
                //2：已经成功的任务，N表无,T表有--更新T表(不处理)
            }
            else if(task[2].equals("F"))
            {
                jedis.srem("task",temp_task);//移除此失败任务
                System.out.println("移除评定失败任务:"+temp_task);
                FindById(task[0],task_N,true);//从未评定的等待列表中移除
                //重新生成目的地
                String free_analysis=get_a_free_analysis();
                if(free_analysis!=null)
                {
                    sendMessage("Analysis"+free_analysis,"locate,"+task[0]);
                }
            }
        }

    }

    //处理车新状态
    public void car_arrive()
    {
        //获取最新的车状态
        Iterator<String> iterator=jedis.smembers("cars").iterator();
        while(iterator.hasNext())
        {
            //遍历新车状态表
            String car=iterator.next();
            String car_more[]=car.split(",");
            if(car_more[3].equals("M"))
            {
                //车移动,删除等待列表
                FindById(car_more[0],car_W,true);
            }
            else if(car_more[3].equals("W"))
            {
                //车停
                //1:初次停:w中没有，则加入，并判断是否需要生成目的地，并调度导航器
                if(!FindById(car_more[0],car_W,false))
                {   //加入
                    car_W.add(car_more[0]+","+wait_time);
                    String free_analysis=get_a_free_analysis();
                    if(free_analysis!=null)
                    {
                        sendMessage("Analysis"+free_analysis,"locate,"+car_more[0]);
                    }
                    String free_navigate=get_a_free_navigate();
                    if(free_navigate!=null)
                    {
                        sendMessage("Navigate" +free_navigate, car_more[0]);
                    }
                }
                //2:在列表:w中有，则刷新时间(不需要操作)
            }

        }
    }

    //控制器结束工作
    public void endControl()
    {
        //关闭发送器
        this.sender.StopWork();
        this.sender=null;
        //标志转换
        isWork=false;
        //清数据区
        jedis.del("navigates");
        jedis.del("analysis_state");
        jedis.del("lock");
        //提示，并结束程序
        System.out.println("控制器正在停止运行");
        System.exit(0);
    }

    //向指定组件发消息
    public void sendMessage(String component,String message)
    {
        //component可能为：Car+车编号、Navigate+导航器编号、Analysis+分析器编号、recall+回放器编号、view+显示器编号
        sender.SendAMsg(component,message,"");
        System.out.println("控制器通知"+component+":"+message);
    }
}