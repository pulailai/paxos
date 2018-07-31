#encoding:utf-8
# #追随者
from Message import Message #消息
from MessagePump import MessagePump #消息传送
from InstanceRecord import InstanceRecord #本地记录仪
from  PaxosAcceptorProtocol import PaxosAcceptorProtocol  #追随者协议


class PaxosAcceptor:
    def __init__(self, port, leaders):#初始化
        self.port = port#端口
        self.leaders = leaders#领导者
        self.instances = {}#接口列表
        self.msgPump = MessagePump(self, self.port)#消息传送器
        self.failed = False#没有失败

    def start(self):#开始
        self.msgPump.start()

    def stop(self):#停止
        self.msgPump.doAbort()

    def fail(self):#失败
        self.failed = True

    def recover(self):#恢复
        self.failed = False

    def sendMessage(self, message):#发送消息
        self.msgPump.sendMessage(message)

    def recvMessage(self, message):#收取消息
        if message == None: #消息为空
            return
        if self.failed:#失败状态不再收取消息
            return
        if message.command == Message.MSG_PROPOSE:#判断消息是不是提议
            if message.instanceID not in self.instances:
                record = InstanceRecord()#记录器
                self.instances[message.instanceID] = record#记录下来
            protocol = PaxosAcceptorProtocol(self)#创建协议
            protocol.recvProposal(message)#收取消息
            self.instances[message.instanceID].addProtocol(protocol)#记录协议
        else:
            # 抓取记录
            self.instances[message.instanceID].getProtocol(message.proposalID).doTransition(message)

    def notifyClient(self, protocol, message):#通知客户端
        if protocol.state == PaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED:
            self.instances[protocol.instanceID].value = message.value#存储信息
            print(u"协议被客户端接受 %s" %message.value)

    def getHighestAgreedProposal(self, instance):#获取最高同意的建议
        return self.instances[instance].highestID

    def getInstanceValue(self, instance):#获取接口数据
        return self.instances[instance].value