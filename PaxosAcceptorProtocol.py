#encoding:utf-8
# #追随者协议
from  Message import Message  #协议依赖消息
class PaxosAcceptorProtocol(object):
    # State variables
    STATE_UNDEFINED = -1 #协议没有定义的情况0
    STATE_PROPOSAL_RECEIVED = 0  #收到消息
    STATE_PROPOSAL_REJECTED = 1#拒绝链接
    STATE_PROPOSAL_AGREED = 2  #同意链接
    STATE_PROPOSAL_ACCEPTED = 3  #同意请求
    STATE_PROPOSAL_UNACCEPTED = 4#拒绝请求

    def __init__(self, client):#初始化
        self.client = client#客户端
        self.state = PaxosAcceptorProtocol.STATE_UNDEFINED#未定义

    def recvProposal(self, message):#收取
        if message.command == Message.MSG_PROPOSE:#处理协议
            self.proposalID = message.proposalID#协议编号
            self.instanceID = message.instanceID
            (port, count) = self.client.getHighestAgreedProposal(message.instanceID)
            # 监测编号处理消息协议
            # 判断协议是不是最高的
            if count < self.proposalID[0] or (count == self.proposalID[0] and port < self.proposalID[1]):
                self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_AGREED#协议统一
                print ("同意协议:%s, %s "%(message.instanceID, message.value) )
                value = self.client.getInstanceValue(message.instanceID)#抓取数据
                msg = Message(Message.MSG_ACCEPTOR_AGREE)#同意协议
                msg.copyAsReply(message)#拷贝并回复
                msg.value = value#保存值
                msg.sequence = (port, count)#保存数据
                self.client.sendMessage(msg) #发送消息
            else:
                self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_REJECTED
                # 拒绝状态
            return self.proposalID
        else:
            # 错误重新尝试
            pass

    def doTransition(self, message):#过度
        if self.state == PaxosAcceptorProtocol.STATE_PROPOSAL_AGREED and message.command == Message.MSG_ACCEPT:
            self.state = PaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED#接收协议
            msg = Message(Message.MSG_ACCEPTOR_ACCEPT)#创造消息
            msg.copyAsReply(message)#拷贝并回复
            for l in self.client.leaders:
                msg.to = l
                self.client.sendMessage(msg)#给领导发送消息
            self.notifyClient(message)#通知自己
            return True

        raise  Exception(u"并非预期的状态与命令")

    def notifyClient(self, message): #通知 自己客户端
        self.client.notifyClient(self, message)