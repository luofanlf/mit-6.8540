# mit-6.5840

**wc.go**：适用于wordcount的map和reduce函数，运行go build -buildmode=plugin ../mrapps/wc.go后生成wc.so文件在其他文件中被动态的加载进来，因此map以及reduce的实现方式不再任务要求内，只需要运行的时候加载插件即可

**mrwoker.go**：加载map和reduce方法，初始化一个worker

### rpc调用：

首先需要实现coordianator和worker之间的通信，由于是进程间的通信，所以需要采用rpc调用（线程中的通信可以采用channel）

rpc的实现通过使用了Unix Domain Socket进行监听，HTTP协议传输数据

* worker向coordinator请求任务
* worker向coordinator报告任务完成

   

在rpc中定义了一些请求和响应结构体

完成了rpc的部分，大致了解了rpc调用的过程，开始写worker ask for a task，遇到了一个问题：如何标识当前work的workid呢,初始化worker：用uuid给workid赋值

请求发到了coordinator这边，不知道该怎么返回，决定先初始化一些coordinator的结构体

文件分片：暂定按输入文件分割，有几个输入文件就分割成多少个map任务



问题：调试的过程中，起了一个coordinator和两个worker，结果另一个worker还没起来第一个worker就把任务全都做完了

解决方案：在coodinator ask for task函数的开始添加短暂延迟



map任务完成，生成健值对，需要存储在本地磁盘中：采用文件的格式保存，保存为mr-temp+maptaskid +reducetaskid的形式，应该用json的形式将intermediate value保存在文件中



在map执行完了以后，需要report：为了校验是否所有的map任务都被执行完并且能够让coordinator进行reduce任务的制作



Exist_early test fail:work提早退出了导致报错，在work做完了所有reduce任务之后，在一个for循环中一直pin coordinator，等待coordinator退出后再退出



test_crash:在coordinator端，用一个map保存workerid和taskid的对应关系，在worker端另起一个go routine来持续向coordinator发送心跳