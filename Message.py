# encoding:utf-8
#  #消息传递类
class Message:
    # 常量
    MSG_ACCEPTOR_AGREE = 0  # 追随者同意
    MSG_ACCEPTOR_ACCEPT = 1  # 追随者接受
    MSG_ACCEPTOR_REJECT = 2  # 追随者拒绝-网络不通
    MSG_ACCEPTOR_UNACCEPT = 3  # 追随者网络通-不同意
    MSG_ACCEPT = 4  # 接受
    MSG_PROPOSE = 5  # 提议
    MSG_EXT_PROPOSE = 6  # 额外提议
    MSG_HEARTBEAT = 7  # 心跳，每隔一段时间同步消息

    def __init__(self, command=None):  # 消息初始化会有一个命令
        self.command = command

    def copyAsReply(self, message): #回复
        # 提议ID  #当前的ID  #发给谁  #谁发的
        self.proposalID = message.proposalID,  # 提议ID
        self.instanceID = message.instanceID,  # 当前的ID
        self.to = message.to,  # 发给谁
        self.source = message.source,  # 谁发的
        self.value = message.value  # 发的信息
