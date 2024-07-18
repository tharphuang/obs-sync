# obs-sync
# 使用
1.部署server 和client

注：--log是日志文件的位置，其父目录需存在。client与sever在同一节点时，client的svr地址为0.0.0.0。client单独部署时此处为server服务的IP地址
```
 nohup ./bin/server --log=./svr.log &
 nohup ./bin/client --svr=0.0.0.0 --log=./svr.log &
```

2.使用
发起一个同步任务
obsync 源ak:源sk@源云产品类型://源云区域  目的ak:目的sk@目的云产品类型://目的云区域
```
./bin/obsync sync ak:sk@cuc://nxyc  ak:sk@cuc://helf
```
开始同步任务
```
./bin/obsync start
```
查看同步任务统计信息
```
./bin/onsync stat
```
