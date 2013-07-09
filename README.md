#n3r-flume-ng

Apache flume ng的扩展和定制化（当前基于Flume 1.3.1）

#ExecBlockSource

* ExecSource的改进版本
* 增加Flush events to channel的Timeout控制，默认的Timeout为3000毫秒
* 支持按block进行分隔Exec output，block通过正则表达式进行识别；若未指定对应的正则表达式，则按回车换行符分隔Exec output