# Lab2-key/value server

### Linearizability

线性一致性：

- Operation瞬间完成（或者原子性，发生在 Inv 和 Resp 两个事件之间）
- 一个进程对共享变量的修改后其他进程所有读操作必须为共享变量的最新的值。在读取操作之后的任意读取操作都应返回相同的结果，直到下一次写入成功。

参考博客：

https://zhuanlan.zhihu.com/p/42239873

https://cn.pingcap.com/blog/linearizability-and-raft/

https://anishathalye.com/testing-distributed-systems-for-linearizability/



### KeyValue Sever without network failure

非常简单，根据逻辑实现get和put两个方法，在调用方法的前后用mu对于kv上锁，需要在server的结构体中定义一个version来记录版本



### KeyValue Sever With Dropped Message

client发起rpc请求-> server处理请求->client接受server返回的response

在此过程之中，可能因为消息丢失而导致client没有接收到server返回的消息，因此需要给client加上重发消息的机制

- 需要给每次请求加上唯一标志符从而server不会对同一个request重复处理--请求参数字段加上一个requestID，利用uuid生成

- server端map存储请求id和reply的映射关系

- server端校验如果request的version和当前的kv中存储的version不一致，返回ErrMaybe

  ```go
  func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
    args := rpc.PutArgs{
      RequestId: uuid.New().String(),
      Key:       key,
      Value:     value,
      Version:   version,
    }
    reply := rpc.PutReply{}
  ​
    for {
      ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
      if ok {
        // RPC 调用成功，直接返回结果
        return reply.Err
      }
      // RPC 调用失败，等待后重试
      time.Sleep(100 * time.Millisecond)
    }
  }
  ​
  
  ```

  