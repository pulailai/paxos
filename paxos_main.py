#encoding:utf-8
import threading, socket, pickle,random #多线程，网络通信，对象存取，队列，随机数
from multiprocessing import Queue #队列
from MessagePump import MessagePump  #信息传送
from  Message import Message #消息
from  InstanceRecord import InstanceRecord #日志记录
from  PaxosLeader import PaxosLeader #领导者
from PaxosLeaderProtocol import PaxosLeaderProtocol #领导者协议
from  PaxosAcceptorProtocol import PaxosAcceptorProtocol #追随者协议
from PaxosAcceptor import PaxosAcceptor #追随者
import time #时间

if __name__ == '__main__':
    #设定5个客户端
    numclients=5
    clients=[PaxosAcceptor(port,[54321,54322]) for  port  in range(64320,64320+numclients)]
    #两个领导者
    leader1=PaxosLeader (54321,[54322],[c.port for c in clients])
    leader2 = PaxosLeader(54322, [54321], [c.port for c in clients])

    #开启领导者与追随者
    leader1.start()
    leader1.setPrimary(True)
    leader2.setPrimary(True)
    leader2.start()
    for  c in clients:
        c.start()

    #破坏,客户端不链接
    clients[0].fail()
    clients[1].fail()

    #通信
    s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)#udp协议
    start=time.time()
    for  i  in range(1000):
        m=Message(Message.MSG_EXT_PROPOSE)#消息
        m.value=0+i #消息参数
        m.to=54322#设置传递的端口
        bytes=pickle.dumps(m) #提取的二进制数据
        s.sendto(bytes,("localhost",m.to)) #发送消息

    while leader2.getNumAccepted()<999:
        print ("休眠的这一秒 %d "%leader2.getNumAccepted())
        time.sleep(1)

    print (u"休眠10秒")
    time.sleep(10)
    print (u"停止leaders")
    leader1.stop()
    leader2.stop()
    print (u"停止客户端")
    for  c in clients:
        c.stop()

    print (u"leader1历史纪录")
    print (leader1.getHistory())
    print (u"leader2历史纪录")
    print (leader2.getHistory())


    end=time.time()
    print (u"一共用了%f秒" %(end- start))
