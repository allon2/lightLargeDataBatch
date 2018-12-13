# **LargeDataBatch**  
## Introduction
LargeDataBatch是一个轻量级大数据处理工具,提供简单易用的API。
## Overview
在项目开发过程中,经常会遇到大数据问题处理问题，比如上G的数据需要入库,同时对数据的处理
通常会有内存限制和处理时间要求。基于此,开发此代码,可通过调整参数达到要求。


## Features
- 1、通过调整线程数和批次数，可调节数据库CPU的高低，充分压榨数据库
- 2、默认提供对Mybatis的支持
- 3、提供定时读取和处理功能,适合比如读取MQ等不确定信息，但需要轮训并及时处理的情况
- 4、自动对数据进行分片
- 5、充分利用线程池,避免空闲线程
- 6、数据处理完毕后，可继续执行同步交易

## Release Note
 1.0.3版本
- 1、一个JVM可公用一个线程池
- 2、增加shutdown方法，关闭线程池
- 3、增加setThreadpool方法和setScheduleservice方法，外部可传入线程池，如果不传入,使用默认线程池。注意传入线程池线程数需要大于或者等于可能的总的信号量

## Getting Started
### Maven dependency

```xml
<dependency>
    <groupId>cn.ymotel</groupId>
    <artifactId>largedatabtach</artifactId>
    <version>1.0.3</version>
</dependency>
```
### Gradle dependency

```
compile group: 'cn.ymotel', name: 'largedatabtach', version:'1.0.3'

```
```
       MybatisBatchDataConsumer mybatisbatch=new MybatisBatchDataConsumer();
       
        mybatisbatch.setSqlSessionFactory(sqlSession);
        
        LargeDataBatch batchhelp=new ThreadSafeLargeDataBatchHelp();
        
        batchhelp.init(100,10,mybatisbatch);
        
        MybatisResultHandler result=new MybatisResultHandler();
        
        result.setSql("xx");
        
        result.setDatabatch(batchhelp)
        
        sqlSession.select("xxx",result);
        
        batchhelp.end();
代码调用逻辑:  
- 1、 batchhelp.init(100,10,mybatisbatch) 初始化批次数，线程数和数据处理类.
- 2、使用sqlSession.select()方法，调用MybatisResultHandler，将取得的数据放入batchhelp的队列中,
- 3、在batchhelp中的数据达到一个批次后，将数据和数据处理类推送给消费者(MybatisBatchDataConsumer)进行处理
- 4、 最后调用end方法，将队列中不够一个批次的数据推送给消费者类(MybatisBatchDataConsumer)进行处理
## Principle
    在数据提供者提供数据后，将数据放入队列,如果队列达到批次数，将数据提交给线程池。
    线程池有空闲队列，消费数据。线程池无空闲队列,阻塞主线程，防止过量获取数据。
    线程池有空闲线程后，将队列数据再此提交给线程,同时唤醒主线程，使得主线程可继续提供数据

## Usage
    
   数据提供者可以是数据库,也可以是文件或者其他类型,多种多样,只要能调用addData或者addSql方法即可。
   数据提供者和消费者可自由组合
 BatchDataConsumer接口介绍
 
      消费者可通过spring配置或者通过new方法实例化进行定义。
      程序得到消费者对象后，会生成多个副本，以提高性能
      消费者类需要实现BatchDataConsumer接口，程序中因为将会生成多个副本,程序在调用end方法后
      ，会销毁程序中的副本，为了避免内存泄漏,请注意在close方法中关闭相应对象
      程序会调用Runnable的run方法进行数据处理

      
      
  ThreadSafeLargeDataBatchHelp 方法介绍:
  
    LargeDataBatch 目前有LargeDataBatchHelp和ThreadSafeLargeDataBatchHelp两个实现类
    ,推荐使用ThreadSafeLargeDataBatchHelp
    ThreadSafeLargeDataBatchHelp可在spring 中进行配置，供其他类引用。
     
     /**
      * 设置在一个jvm内的总的最高可并行的线程数,
      * 在一个jvm中可能会多个地方调用ThreadSafeLargeDataBatchHelp类。
      * 如果不加控制，可能导致应用服务器在同一时间线程数过大,应用服务器处理异常情况发生，
      * 如果消费者同时超过数据库也可能导致数据库超过阈值情况发生
      *不设置,则不会对线程池中的线程数进行总体控制
      * @param totalThreadCount
      */
     public void setTotalThread(int totalThreadCount) {
         
      init方法中参数介绍：
      beanName是spring中配置的beanName，需配置为singleton="false",需要实现BatchDataInterface 接口 
      使用此参数需要ThreadSafeLargeDataBatchHelp可得到ApplicationContext对象，
      推荐在spring中进行自动注入，
      batchsize在达到批次数后，会将一批数据整体提交到线程池进行处理,
      在数据库中为了提高性能，对大数据的处理一般都是批次提交，
      通过开启此batchsize后，可将数据自动分组
      threadsize线程数,开启多少个线程同时处理此数据,
      如果是数据处理结果是数据库入库操作，通过调整此大小，可提高或者降低数据库的CPU
      timeout  超过时间，线程会将未达阀值数据自动提交，如果不设置超时时间
      ，则默认不开启。
      需要定时轮训读取流信息，无法调用end方法的场景可通过增加此参数，实现自动提交
      BatchDataInterface t 是通过new 方法实例化的bean
      /**
         * @param batchsize  在thread中的每次执行条数
         * @param threadsize 同时并发的线程数
         * @param beanName   在配置文件中改Aciton 需配置为singleton="false"
         */
        public void init(int batchsize, int threadsize, String beanName) 
        
        /**
         * @param batchsize  在thread中的每次执行条数
         * @param threadsize 同时并发的线程数
         * @param beanName    需配置为singleton="false"
         * @param timeout    超过时间，线程会将未达阀值数据，自动提交
         */
          public void init(int batchsize, int threadsize, String beanName, long timeout) 

        /**
         * @param batchsize  在thread中的每次执行条数
         * @param threadsize 同时并发的线程数
         * @param t   实现 BatchDataInterface 的对象
         */
        public void init(int batchsize, int threadsize, BatchDataInterface t) 
        /**
         * @param batchsize  在thread中的每次执行条数
         * @param threadsize 同时并发的线程数
         * @param t   实现 BatchDataInterface 的对象
         * @param timeout    超过时间，线程会将未达阀值数据，自动提交
         */
        public void init(int batchsize, int threadsize, BatchDataInterface t, long timeout) {           
        addSql方法介绍：
            程序会将sql和obj数据组成一个数组,放入队列中，在达到阈值将数据放入List中供消费者调用
        /**
         * 程序会将sql和数据组成一个数组,放入队列中，供消费者调用
         * @param sql
         * @param obj
         */
        public void addSql(String sql, Object obj)  {
        /**
         * 程序会将obj,放入队列中，在达到阈值将数据放入List中供消费者调用
         * @param obj
         */
        public void addData(Object obj){
        /**
         * 数据处理结尾一般会剩余一些未达到batchsize的数据未处理,通过调用此方法
         *,可将未达到阈值的数据提交到线程池中进行处理
         */
        public void end() {
        /**
        *关闭线程池
        */
        public void shutdown
      
    
     