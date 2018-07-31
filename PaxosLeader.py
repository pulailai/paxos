#encoding:utf-8
# #领导者
from multiprocessing import Queue #队列
import time #时间

from pip._vendor.urllib3.connectionpool import xrange

from Message import Message #消息
from MessagePump import MessagePump #消息传送
from InstanceRecord import InstanceRecord #本地记录仪
from  PaxosAcceptorProtocol import PaxosAcceptorProtocol  #追随者协议
from PaxosLeaderProtocol import PaxosLeaderProtocol #领导者协议
import threading #多线程
class PaxosLeader:
    def __init__(self, port, leaders=None, acceptors=None):#初始化
        self.port = port  #端口号
        if leaders == None:  #初始化领导者
            self.leaders = []
        else:
            self.leaders = leaders
        if acceptors == None:#初始化追随者
            self.acceptors = []
        else:
            self.acceptors = acceptors
        self.group = self.leaders + self.acceptors  #组
        self.isPrimary = False   #自身是不是领导
        self.proposalCount = 0  #协议的数量
        self.msgPump = MessagePump(self, port)  #消息传送器
        self.instances = {} #接口，实例，
        self.hbListener = PaxosLeader.HeartbeatListener(self) #监听
        self.hbSender = PaxosLeader.HeartbeatSender(self)  #发送
        self.highestInstance = -1 #协议状态
        self.stopped = True  #是否正在运
        self.lasttime = time.time()   #最后一次的时间

    class HeartbeatListener(threading.Thread):#定时监听
        def __init__(self, leader):
            self.leader = leader #领导者
            self.queue = Queue() #队列
            self.abort = False
            threading.Thread.__init__(self) #父类初始化

        def newHB(self, message): #队列取出数据
            self.queue.put(message)

        def doAbort(self):  #放弃
            self.abort = True

        def run(self): #开始执行，读取消息
            elapsed = 0 #时间计数器
            while not self.abort:
                s = time.time() #取得时间
                try:
                    hb = self.queue.get(True, 2)#抓取消息，2超时的时间
                    #设定规则，谁的端口号比较高，谁就是领导
                    if hb.source > self.leader.port:
                        self.leader.setPrimary(False)
                except:  # 异常，设置自身为领导
                    self.leader.setPrimary(True)

    class HeartbeatSender(threading.Thread): #定时发送
        def __init__(self, leader):
            self.leader = leader
            self.abort = False
            threading.Thread.__init__(self)

        def doAbort(self):
            self.abort = True

        def run(self):
            while not self.abort:
                time.sleep(1)
                if self.leader.isPrimary:
                    msg = Message(Message.MSG_HEARTBEAT)#心跳的消息
                    msg.source = self.leader.port#设置端口
                    for l in self.leader.leaders:#循环领导
                        msg.to = l
                        self.leader.sendMessage(msg)#发送消息

    # ------------------------------------------------------
    def sendMessage(self, message):#发送消息
        self.msgPump.sendMessage(message)

    def start(self):#开始，心跳监听，发送，发送器启动
        self.hbSender.start()
        self.hbListener.start()
        self.msgPump.start()
        self.stopped = False

    def stop(self): #停止
        self.hbSender.doAbort() # 心跳监听，发送，发送器停止
        self.hbListener.doAbort()
        self.msgPump.doAbort()
        self.stopped = True

    def setPrimary(self, primary):#设置领导者
        if self.isPrimary != primary:
            # Only print if something's changed
            if primary:
                print(u"我是leader%s" % self.port)
            else:
                print(u"我不是leader%s" % self.port)
        self.isPrimary = primary

    # ------------------------------------------------------

    def getGroup(self): #获取所有的领导下面的追随者
        return self.group

    def getLeaders(self):#获取所有的领导
        return self.leaders

    def getAcceptors(self):#获取所有的追随者
        return self.acceptors

    def getQuorumSize(self):#必须获得1/2以上的人支持
        return (len(self.getAcceptors()) / 2) + 1

    def getInstanceValue(self, instanceID):  # 获取接口数据
        if instanceID in self.instances:
            return self.instances[instanceID].value
        return None

    def getHistory(self):#抓取历史纪录
        return [self.getInstanceValue(i) for i in xrange(1, self.highestInstance + 1)]

    def getNumAccepted(self):#抓取同意的数量
        return len([v for v in self.getHistory() if v != None])

    # ------------------------------------------------------

    def findAndFillGaps(self):#抓取空白时间处理下事务
        # if no message is received, we take the chance to do a little cleanup
        for i in xrange(1, self.highestInstance):
            if self.getInstanceValue(i) == None:
                print(u"填充空白", i)
                self.newProposal(0,  i)
        self.lasttime = time.time()

    def garbageCollect(self):#采集无用信息
        for i in self.instances:
            self.instances[i].cleanProtocols()

    def recvMessage(self, message): #通知领导

        if self.stopped:
            return   #停止就不干活了
        if message == None:
            if self.isPrimary and time.time() - self.lasttime > 15.0:
                self.findAndFillGaps()
                self.garbageCollect() #处理
            return
        if message.command == Message.MSG_HEARTBEAT:  #处理心跳信息
            self.hbListener.newHB(message)
            return True
        if message.command == Message.MSG_EXT_PROPOSE:
            print(u"额外的协议 %s,%s"  %( self.port, self.highestInstance))
            #print ("External proposal received at", self.port, self.highestInstance)
            if self.isPrimary:
                self.newProposal(message.value)#新的协议
            return True
        if self.isPrimary and message.command != Message.MSG_ACCEPTOR_ACCEPT:
            self.instances[message.instanceID].getProtocol(message.proposalID).doTransition(message)

        if message.command == Message.MSG_ACCEPTOR_ACCEPT:
            if message.instanceID not in self.instances:
                self.instances[message.instanceID] = InstanceRecord()
            record = self.instances[message.instanceID]#记录
            if message.proposalID not in record.protocols:#创建协议
                protocol = PaxosLeaderProtocol(self)
                protocol.state = PaxosLeaderProtocol.STATE_AGREED
                protocol.proposalID = message.proposalID
                protocol.instanceID = message.instanceID
                protocol.value = message.value
                record.addProtocol(protocol)
            else:
                protocol = record.getProtocol(message.proposalID)#取出
            protocol.doTransition(message)#处理
        return True

    def newProposal(self, value, instance=None):#新的提议
        protocol = PaxosLeaderProtocol(self)#创建协议
        if instance == None:
            self.highestInstance += 1
            instanceID = self.highestInstance
        else:
            instanceID = instance
        self.proposalCount += 1#协议追加
        id = (self.port, self.proposalCount)#保存端口，协议协议
        if instanceID in self.instances: #记录
            record = self.instances[instanceID]
        else:
            record = InstanceRecord()
            self.instances[instanceID] = record
        protocol.propose(value, id, instanceID)#协议创建处理
        record.addProtocol(protocol)#追加协议

    def notifyLeader(self, protocol, message):
        # 通知领导
        if protocol.state == PaxosLeaderProtocol.STATE_ACCEPTED:
            print(u"协议接口%s  被%s 接受" % (message.instanceID, message.value))
            self.instances[message.instanceID].accepted = True
            self.instances[message.instanceID].value = message.value
            self.highestInstance = max(message.instanceID, self.highestInstance)
            return
        if protocol.state == PaxosLeaderProtocol.STATE_REJECTED:
            # 重新尝试
            self.proposalCount = max(self.proposalCount, message.highestPID[1])
            self.newProposal(message.value)
            return True
        if protocol.state == PaxosLeaderProtocol.STATE_UNACCEPTED:
            pass