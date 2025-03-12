# mit-6.5840

**wc.go**：适用于wordcount的map和reduce函数，运行go build -buildmode=plugin ../mrapps/wc.go后生成wc.so文件在其他文件中被动态的加载进来

### rpc调用：

首先需要实现coordianator和worker之间的通信，由于是进程间的通信，所以需要采用rpc调用（线程中的通信可以采用channel）

rpc的实现通过使用了Unix Domain Socket进行监听，HTTP协议传输数据

* worker向coordinator请求任务
* worker向coordinator报告任务完成