#encoding:utf-8
import threading, socket, pickle,random
#  InstanceRecord本地记录类，追随者，领导者之间协议
from multiprocessing import Queue
from  PaxosLeaderProtocol import PaxosLeaderProtocol
class InstanceRecord:
    def __init__( self ):#初始化
        self.protocols = {} #协议字典
        self.highestID = (-1 ,-1) #最高的编号
        self.value = None
    def addProtocol( self, protocol ):#增加协议
        self.protocols[ protocol.proposalID ] = protocol #追加协议
        if protocol.proposalID[1] > self.highestID[1] or \
                (protocol.proposalID[1] == self.highestID[1] and protocol.proposalID[0] > self.highestID[0]):
            self.highestID = protocol.proposalID#取得编号最多的协议

    def getProtocol(self, protocolID):#根据编号抓取协议
        return self.protocols[protocolID]

    def cleanProtocols(self):#清理协议
        keys = self.protocols.keys()#取得所有key
        for k in keys:
            protocol = self.protocols[k] #取得协议
            if protocol.state == PaxosLeaderProtocol.STATE_ACCEPTED:
                print("Deleting protocol")
                del self.protocols[k] #删除协议