#encoding:utf-8
from MessagePump import MessagePump
import random
class AdversarialMessagePump( MessagePump ):#类的继承
    # 对抗消息传输，延迟消息并任意顺序传递,模拟网络的延迟，消息传送并不是顺序
    def __init__( self, owner, port, timeout=2 ):
        MessagePump.__init__( self, owner, port, timeout ) #初始化父类
        self.messages = set( )#集合避免重复

    def waitForMessage( self ):
        try:
            msg = self.queue.get( True, 0.1 )#从队列抓取数据
            self.messages.add( msg )#添加消息
        except Exception as e:#处理异常
            pass
            # print(e)
        if len(self.messages) > 0 and random.random( ) < 0.95: # Arbitrary!
            msg = random.choice( list( self.messages ) )#随机抓取消息发送
            self.messages.remove( msg )#删除消息
        else:
            msg = None
        return msg