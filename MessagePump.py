#encoding:utf-8
# #基于socket传递消息，封装网络类传递消息 消息接收
import threading #线程
import pickle #对象序列化
import socket #网络信心传输
# import Queue #队列
from multiprocessing import Queue #队列
class MessagePump(threading.Thread):

    class MPHelper(threading.Thread):#传递消息，封装

        def __init__(self, owner):#初始化,#owner属于谁
            self.owner = owner #owner属于谁
            threading.Thread.__init__(self)#父类初始化

        def run(self):#运行
            while not self.owner.abort:#只要所有者线程没有结束
                try:
                    # 返回二进制数据，返回地址
                    (bytes, addr) = self.owner.socket.recvfrom(2048) #收取消息
                    msg = pickle.loads(bytes)#读取二进制数据转化消息
                    msg.source = addr[1]#取出地址
                    self.owner.queue.put(msg)#队列存入消息
                except Exception as e:  # 异常
                    pass
                    # print(e)

    def __init__(self, owner, port, timeout=2):#初始化
        self.owner = owner#所有者
        threading.Thread.__init__(self)#初始化
        self.abort = False  #没有终止
        self.timeout = 2 #超时时间
        self.port = port #网络通信的接口
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)#UDP通信
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 200000)#通信参数
        self.socket.bind(("localhost", port))#通信地址，ip，端口
        self.socket.settimeout(timeout)#超时设置
        self.queue = Queue()#队列
        self.helper = MessagePump.MPHelper(self)#接收消息

    def run(self):#运行主线程
        self.helper.start()#开启收消息的线程
        while not self.abort:
            message = self.waitForMessage() #阻塞等待消息

            self.owner.recvMessage(message)#收取消息

    def waitForMessage(self):#等待消息
        try:
            msg = self.queue.get(True, 3)#抓取数据最多等3秒
            return msg
        except:
            return None

    def sendMessage(self, message):#发送消息
        bytes = pickle.dumps(message)#消息转化为二进制
        address = ("localhost", message.to)#地址ip,端口
        self.socket.sendto(bytes, address)#发送消息
        return True

    def doAbort(self): #不想活了，设置状态为放弃
        self.abort = True



