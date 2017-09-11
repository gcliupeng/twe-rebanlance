# twe-rebanlance
twe-rebanlance 是twemproxy集群的迁移、扩容工具。解决目前twemproxy集群扩容难的问题。
作为一种redis集群实现方式，twemproxy广泛使用，但使用twemproxy有一大不方便之处，就是如果要扩容、迁移老的集群，会非常困难。本工具就是为了解决此问题，它
可以方便、快速、平滑的对twemproxy集群进行扩容、迁移。

使用方式：git clone
         make
         ./twemproxy-rebanlance -o old.yml -n new.yml -c conf.yml
         
使用说明：-o 参数表明老的twemproxy集群配置，-n 表示新的twemproxy集群配置，-c 指定工具的配置。工具配置里面有这么几个参数：
  loglevel DEBUG NOTICE WARNING ERROR 
  logfile 日志文件
  prefix 老集群的所有key都会加上此前缀写到新集群了
  filter 老集群已此开头的key才会迁移。
  
目前集群已在实际线上使用，迁移70g数据时所需10分钟。
