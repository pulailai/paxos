#encoding:utf-8
# #领导者协议
from Message import Message
class PaxosLeaderProtocol(object):
    STATE_UNDEFINED = -1  # 协议没有定义的情况0
    STATE_PROPOSED = 0  # 协议消息
    STATE_REJECTED = 1  # 拒绝链接
    STATE_AGREED = 2  # 同意链接
    STATE_ACCEPTED = 3  # 同意请求
    STATE_UNACCEPTED = 4  # 拒绝请求

    def __init__(self, leader):
        self.leader = leader#领导者
        self.state = PaxosLeaderProtocol.STATE_UNDEFINED#未定义
        self.proposalID = (-1, -1)#初始化，网络好坏的情况，同意拒绝的情况
        self.agreecount, self.acceptcount = (0, 0)
        self.rejectcount, self.unacceptcount = (0, 0)
        self.instanceID = -1
        self.highestseen = (0, 0) #最高的协议

    def propose(self, value, pID, instanceID): #提议
        self.proposalID = pID
        self.value = value
        self.instanceID = instanceID#初始化
        # 创建提议消息
        message = Message(Message.MSG_PROPOSE)#提议消息
        message.proposalID = pID
        message.instanceID = instanceID
        message.value = value


        for server in self.leader.getAcceptors():#遍历服务器
            message.to = server
            self.leader.sendMessage(message)
        self.state = PaxosLeaderProtocol.STATE_PROPOSED#协议消息
        return self.proposalID

    def doTransition(self, message):
        # 过度
        # 根据状态机运行协议
        if self.state == PaxosLeaderProtocol.STATE_PROPOSED:
            if message.command == Message.MSG_ACCEPTOR_AGREE:#同意协议
                self.agreecount += 1
                if self.agreecount >= self.leader.getQuorumSize():#选举
                    print (u"达成协议的法定人数，最后的价值回答是:%s" % message.value)
                    if message.value != None:
                        if message.sequence[0] > self.highestseen[0] or (
                                message.sequence[0] == self.highestseen[0] and message.sequence[1] > self.highestseen[
                            1]):
                            self.value = message.value #数据同步
                            self.highestseen = message.sequence
                    self.state = PaxosLeaderProtocol.STATE_AGREED#同意更新
                    # 发送同意消息
                    msg = Message(Message.MSG_ACCEPT)
                    msg.copyAsReply(message)
                    msg.value = self.value
                    msg.leaderID = msg.to
                    for s in self.leader.getAcceptors():#广播消息
                        msg.to = s
                        self.leader.sendMessage(msg)
                    self.leader.notifyLeader(self, message)#通知leader
                return True
            if message.command == Message.MSG_ACCEPTOR_REJECT:#拒绝
                self.rejectcount += 1
                if self.rejectcount >= self.leader.getQuorumSize():
                    self.state = PaxosLeaderProtocol.STATE_REJECTED #拒绝
                    self.leader.notifyLeader(self, message)#传递消息
                return True
        if self.state == PaxosLeaderProtocol.STATE_AGREED:
            if message.command == Message.MSG_ACCEPTOR_ACCEPT:#同意协议
                self.acceptcount += 1
                if self.acceptcount >= self.leader.getQuorumSize():#投票
                    self.state = PaxosLeaderProtocol.STATE_ACCEPTED#接受
                    self.leader.notifyLeader(self, message)
            if message.command == Message.MSG_ACCEPTOR_UNACCEPT: #不同意的情况
                self.unacceptcount += 1
                if self.unacceptcount >= self.leader.getQuorumSize(): #投票
                    self.state = PaxosLeaderProtocol.STATE_UNACCEPTED
                    self.leader.notifyLeader(self, message)
        pass



