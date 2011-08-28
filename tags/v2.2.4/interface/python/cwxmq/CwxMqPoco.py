#coding:utf-8
'''
@file CwxMqPoco.py
@brief MQ系列服务的接口协议定义
@author 王淼源
@e-mail wmy123452005@sina.com
@version 1.0
@date 2011-06-27
@warning
@bug
'''

from CwxMsgHeader import *
from CwxPackage import *
import binascii
import struct
import zlib
import hashlib
from CwxMqError import *

class CwxMqPoco:

    #协议的消息类型定义
    #RECV服务类型的消息类型定义
    MSG_TYPE_MQ = 1 #<数据提交消息
    MSG_TYPE_MQ_REPLY = 2 #<数据提交消息的回复
    MSG_TYPE_MQ_COMMIT = 3 #<数据commit消息
    MSG_TYPE_MQ_COMMIT_REPLY = 4 #<commit消息的回复
    #分发的消息类型定义
    MSG_TYPE_SYNC_REPORT = 5 #<同步SID点报告消息类型
    MSG_TYPE_SYNC_REPORT_REPLY = 6 #<失败返回
    MSG_TYPE_SYNC_DATA = 7
    MSG_TYPE_SYNC_DATA_REPLY = 8
    #MQ Fetch服务类型的消息类型定义
    MSG_TYPE_FETCH_DATA = 9 #<数据获取消息类型
    MSG_TYPE_FETCH_DATA_REPLY = 10 #<回复数据获取消息类型
    MSG_TYPE_FETCH_COMMIT = 11 #<commit 获取的消息
    MSG_TYPE_FETCH_COMMIT_REPLY = 12 #<reply commit的消息
    #创建mq queue消息
    MSG_TYPE_CREATE_QUEUE = 100 #<创建MQ QUEUE的消息类型
    MSG_TYPE_CREATE_QUEUE_REPLY = 101 #<回复创建MQ QUEUE的消息类型
    #删除mq queue消息
    MSG_TYPE_DEL_QUEUE = 102 #<删除MQ QUEUE的消息类型
    MSG_TYPE_DEL_QUEUE_REPLY = 103 #<回复删除MQ QUEUE的消息类型

    #binlog内部的sync binlogleixing
    GROUP_SYNC = 0XFFFFFFFF 

    #协议的key定义
    KEY_DATA = "data"
    KEY_TYPE = "type"
    KEY_RET = "ret"
    KEY_SID = "sid"
    KEY_ERR = "err"
    KEY_BLOCK = "block"
    KEY_TIMESTAMP = "timestamp"
    KEY_USER = "user"
    KEY_PASSWD = "passwd"
    KEY_SUBSCRIBE = "subscribe"
    KEY_QUEUE = "queue"
    KEY_GROUP = "group"
    KEY_CHUNK = "chunk"
    KEY_WINDOW = "window"
    KEY_M = "m"
    KEY_SIGN = "sign"
    KEY_CRC32 = "crc32"
    KEY_MD5 = "md5"
    KEY_NAME = "name"
    KEY_AUTH_USER = "auth_user"
    KEY_AUTH_PASSWD = "auth_passwd"
    KEY_COMMIT = "commit"
    KEY_TIMEOUT = "timeout"
    KEY_DEF_TIMEOUT = "def_timeout"
    KEY_MAX_TIMEOUT = "max_timeout"
    KEY_COMMIT = "commit"
    KEY_UNCOMMIT = "uncommit"
    KEY_ZIP = "zip"
    KEY_DELAY = "delay"

    
    def __init__(self):
        self.header = CwxMsgHeader()
        self.package = CwxPackage()
    
    def _reset(self):
        self.package.__init__()
        self.header.__init__()

    def __str__(self):
        package_str = str(self.package)
        if CwxMsgHeader.check_attr(self.header.attr, CwxMsgHeader.ATTR_COMPRESS):
            package_str = zlib.compress(package_str)
        self.header.data_len = len(package_str)
        return str(self.header) + package_str

    def parse_header(self, msg_header):
        '''
        @brief 解析消息头，存入self.header中
        @return None
        '''
        if len(msg_header) < CwxMsgHeader.HEAD_LEN:
            raise BadPackageError()
        self.header.__init__(msg_header[:CwxMsgHeader.HEAD_LEN])

    def pack_mq(self, task_id, data, group, type, user=None, passwd=None, sign=None, zip=False):
        '''
        @brief 形成mq的一个消息包
        @param [in] task_id task-id,回复的时候会返回。
        @param [in] data msg的data。
        @param [in] group msg的group。
        @param [in] type msg的type。
        @param [in] user 接收mq的user，若为空，则表示没有用户。
        @param [in] passwd 接收mq的passwd，若为空，则表示没有口令。
        @param [in] sign  接收的mq的签名类型，若为空，则表示不签名。
        @param [in] zip  是否对数据压缩.
        @return 生成的数据包
        '''
        self._reset()
        self.package.data[CwxMqPoco.KEY_DATA] = data
        self.package.data[CwxMqPoco.KEY_GROUP] = group
        self.package.data[CwxMqPoco.KEY_TYPE] = type
        if user:
            self.package.data[CwxMqPoco.KEY_USER] = user
        if passwd:
            self.package.data[CwxMqPoco.KEY_PASSWD] = passwd
        if sign == CwxMqPoco.KEY_CRC32:
            crc = binascii.crc32(str(self.package))
            self.package.data[CwxMqPoco.KEY_CRC32] = struct.pack("i", crc)
        else:
            if sign == CwxMqPoco.KEY_MD5:
                self.package.data[CwxMqPoco.KEY_MD5] = hashlib.md5(str(self.package)).digest()
        if zip:
            self.header.attr = CwxMsgHeader.ATTR_COMPRESS

        self.header.version = 0
        self.header.task_id = task_id
        self.header.msg_type = CwxMqPoco.MSG_TYPE_MQ

        return str(self)

    def parse_package(self, msg):
        self.package.__init__()
        self.package = CwxPackage(msg)
        crc = self.package.get_key(CwxMqPoco.KEY_CRC32)
        if crc:
            crc = struct.unpack("i", crc)[0]
            del self.package.data[CwxMqPoco.KEY_CRC32]
            cal_crc = binascii.crc32(str(self.package))
            if crc != cal_crc:
                raise CwxMqError(CwxMqError.INVALID_CRC32,
                    "CRC32 signture error. recv signture:%x, local signture:%x"
                    % (crc & 0xffffffff, cal_crc & 0xffffffff))
        md5 = self.package.get_key(CwxMqPoco.KEY_MD5)
        if md5:
            del self.package.data[CwxMqPoco.KEY_MD5]
            cal_md5 = hashlib.md5(str(self.package)).digest()
            if md5 != cal_md5:
                raise CwxMqError(CwxMqError.INVALID_MD5,
                    "MD5 signture error. recv signture:%s, local signture:%s"
                    % (binascii.b2a_hex(md5), binascii.b2a_hex(cal_md5)))
    
    
    def parse_mq_reply(self, msg):
        '''
        @brief 解析mq的一个reply消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, sid, err)}
            ret 返回msg的ret。
            sid 返回msg的sid。
            err 返回msg的err-msg。
        '''
        self.parse_package(msg)
        ret = self.package.get_key_int(CwxMqPoco.KEY_RET)
        if ret == None:
            raise CwxMqError(CwxMqError.NO_RET, 
                "No key[%s] in recv page." % CwxMqPoco.KEY_RET)
        sid = self.package.get_key_int(CwxMqPoco.KEY_SID)
        if sid == None:
            raise CwxMqError(CwxMqError.NO_SID,
                "No key[%s] in recv page." % CwxMqPoco.KEY_SID)
        if ret != CwxMqError.SUCCESS:
            err = self.package.get_key(CwxMqPoco.KEY_ERR)
            if err == None:
                raise CwxMqError(CwxMqError.NO_ERR,
                    "No key[%s] in recv page." % CwxMqPoco.KEY_ERR)
        else:
            err = ""

        return OrderedDict((("ret",ret), ("sid",sid), ("err",err)))
    
    def pack_commit(self, task_id, user=None, passwd=None):
        '''
        @brief pack mq的commit消息包
        @param [in] task_id 消息的task-id。
        @param [in] user 接收的mq的user，若为空，则表示没有用户。
        @param [in] passwd 接收的mq的passwd，若为空，则表示没有口令。
        @return 生成的数据包
        '''
        self._reset()
        if user:
            self.package.data[CwxMqPoco.KEY_USER] = user
            if passwd:
                self.package.data[CwxMqPoco.KEY_PASSWD] = passwd
        self.header.msg_type = CwxMqPoco.MSG_TYPE_MQ_COMMIT
        self.header.task_id = task_id

        return str(self)


    def parse_commit_reply(self, msg):
        '''
        @brief 解析mq的一个commit reply消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, err})
            ret 执行状态码。
            err 执行失败时的错误消息。
        '''
        self.parse_package(msg)
        ret = self.package.get_key_int(CwxMqPoco.KEY_RET)
        if ret == None:
            raise CwxMqError(CwxMqError.NO_RET, 
                "No key[%s] in recv page." % CwxMqPoco.KEY_RET)
        if CwxMqError.SUCCESS != ret:
            err_msg = self.package.get_key(CwxMqPoco.KEY_ERR)
            if err_msg == None:
                raise CwxMqError(CwxMqError.NO_ERR,
                        "No key[%s] in recv page." % CwxMqPoco.KEY_ERR)
        else:
            err_msg = ""
        
        return OrderedDict((("ret",ret), ("err",err_msg)))

    def pack_sync_report(self, task_id, sid=None, chunk=0, subscribe=None, user=None, passwd=None, sign=None, zip=False):
        '''
        @brief pack mq的report消息包
        @param [in] task_id task-id。
        @param [in] sid 同步的sid。None表示从当前binlog开始接收
        @param [in] chunk chunk的大小，若是0表示不支持chunk，单位为kbyte。
        @param [in] subscribe 订阅的消息类型。
        @param [in] user 接收的mq的user，若为空，则表示没有用户。
        @param [in] passwd 接收的mq的passwd，若为空，则表示没有口令。
        @param [in] sign  接收的mq的签名类型，若为空，则表示不签名。
        @param [in] zip  接收的mq是否压缩。
        @return 生成的数据包
        '''
        self._reset()
        if sid != None:
            self.package.data[CwxMqPoco.KEY_SID] = sid
        if chunk:
            self.package.data[CwxMqPoco.KEY_CHUNK] = chunk
        if subscribe:
            self.package.data[CwxMqPoco.KEY_SUBSCRIBE] = subscribe
        if user:
            self.package.data[CwxMqPoco.KEY_USER] = user
        if passwd:
            self.package.data[CwxMqPoco.KEY_PASSWD] = passwd
        if sign:
            self.package.data[CwxMqPoco.KEY_SIGN] = sign
        if zip:
            self.package.data[CwxMqPoco.KEY_ZIP] = 1

        self.header.msg_type = CwxMqPoco.MSG_TYPE_SYNC_REPORT
        self.header.task_id = task_id

        return str(self)

    def parse_sync_report_reply(self, msg):
        '''
        @brief parse mq的report失败时的reply消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, sid, err})
            ret report失败的错误代码。
            sid report的sid。
            err report失败的原因。
        '''
        return self.parse_mq_reply(msg)

    def pack_fetch_mq(self, block, queue_name, user=None, passwd=None, timeout=0):
        '''
        @brief pack mq的fetch msg的消息包
        *@param [in] block 在没有消息的时候是否block，1：是；0：不是。
        *@param [in] queue_name 队列的名字。
        *@param [in] user 接收的mq的user，若为空，则表示没有用户。
        *@param [in] passwd 接收的mq的passwd，若为空，则表示没有口令。
        *@param [in] timeout commit队列的超时时间，若为0表示采用默认超时时间，单位为s。
        *@return 生成的数据包
        '''
        self._reset()
        self.package.data[CwxMqPoco.KEY_BLOCK] = block
        if queue_name:
            self.package.data[CwxMqPoco.KEY_QUEUE] = queue_name
        if user:
            self.package.data[CwxMqPoco.KEY_USER] = user
        if passwd:
            self.package.data[CwxMqPoco.KEY_PASSWD] = passwd
        if timeout:
            self.package.data[CwxMqPoco.KEY_TIMEOUT] = timeout

        self.header.msg_type = CwxMqPoco.MSG_TYPE_FETCH_DATA

        return str(self)

    def parse_fetch_mq_reply(self, msg):
        '''
        @brief parse  mq的fetch msg的reply消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, err, sid, timestamp, data, group, type})
            ret 获取mq消息的状态码。
            err 状态不是CWX_MQ_ERR_SUCCESS的错误消息。
            sid 成功时，返回消息的sid。
            timestamp 成功时，返回消息的时间戳。
            data 成功时，返回消息的data。
            group 成功时，返回消息的group。
            type 成功时，返回消息的type。
        '''
        self.parse_package(msg)
        ret = self.package.get_key_int(CwxMqPoco.KEY_RET)
        if ret == None:
            raise CwxMqError(CwxMqError.NO_RET, 
                "No key[%s] in recv page." % CwxMqPoco.KEY_RET)
        if CwxMqError.SUCCESS != ret:
            err_msg = self.package.get_key(CwxMqPoco.KEY_ERR)
            if err_msg == None:
                raise CwxMqError(CwxMqError.NO_ERR,
                        "No key[%s] in recv page." % CwxMqPoco.KEY_ERR)
        else:
            err_msg = ""
        sid = self.package.get_key_int(CwxMqPoco.KEY_SID)
        if sid == None:
            raise CwxMqError(CwxMqError.NO_SID,
                        "No key[%s] in recv page." % CwxMqPoco.KEY_SID)
        time_stamp = self.package.get_key_int(CwxMqPoco.KEY_TIMESTAMP)
        if time_stamp == None:
            raise CwxMqError(CwxMqError.NO_TIMESTAMP, 
                "No key[%s] in recv page." % CwxMqPoco.KEY_TIMESTAMP)         
        if CwxMqPoco.KEY_DATA in self.package.data:
            data = self.package.get_key(CwxMqPoco.KEY_DATA)
        else:
            raise CwxMqError(CwxMqError.NO_KEY_DATA,
                "No key[%s] in recv page." % CwxMqPoco.KEY_DATA)
        
        group = self.package.get_key_int(CwxMqPoco.KEY_GROUP) or 0

        return OrderedDict((("ret",ret), ("err",err_msg), ("sid",sid), 
                ("timestamp",time_stamp), ("data",data), ("group",group),
                ("type",self.package.get_key_int(CwxMqPoco.KEY_TYPE))))

    def pack_fetch_mq_commit(self, commit, delay):
        '''
        @brief pack commit类型队列的commit消息
        @param [in] commit 是否commit。1：commit；0：取消commit
        @param [in] delay uncommit是，delay的秒数。
        @return 生成的数据包
        '''
        self._reset()
        self.package.data[CwxMqPoco.KEY_COMMIT] = commit
        self.package.data[CwxMqPoco.KEY_DELAY] = delay
        
        self.header.msg_type = CwxMqPoco.MSG_TYPE_FETCH_COMMIT

        return str(self)

    def parse_fetch_mq_commit_reply(self, msg):
        '''
        @brief parse  commit类型队列的commit reply消息
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, err})
            ret commit的返回code
            err commit失败时的错误消息
        '''
        return self.parse_commit_reply(msg)

    def pack_create_queue(self, name, user, passwd, scribe, auth_user, auth_passwd, sid, commit, def_time_out=0, max_time_out=0):
        '''
        @brief pack create queue的消息
        @param [in] name 队列的名字
        @param [in] user 队列的用户名
        @param [in] passwd 队列的用户口令
        @param [in] scribe 队列的消息订阅规则
        @param [in] auth_user mq监听的用户名
        @param [in] auth_passwd mq监听的用户口令
        @param [in] sid 队列开始的sid，若为0,则采用当前最大的sid
        @param [in] commit 是否为commit类型的队列，1：是，0：不是。
        @param [in] def_time_out 消息队列的缺省超时时间，若是0，则采用系统默认缺省超时时间。单位为s。
        @param [in] max_time_out 消息队列的最大超时时间，若是0，则采用系统默认最大超时时间。单位为s。
        @return 生成的数据包
        '''
        self._reset()
        self.package.data[CwxMqPoco.KEY_NAME] = name
        self.package.data[CwxMqPoco.KEY_USER] = user
        self.package.data[CwxMqPoco.KEY_PASSWD] = passwd
        self.package.data[CwxMqPoco.KEY_SUBSCRIBE] = scribe
        self.package.data[CwxMqPoco.KEY_AUTH_USER] = auth_user
        self.package.data[CwxMqPoco.KEY_AUTH_PASSWD] = auth_passwd
        self.package.data[CwxMqPoco.KEY_SID] = sid
        self.package.data[CwxMqPoco.KEY_COMMIT] = commit
        self.package.data[CwxMqPoco.KEY_DEF_TIMEOUT] = def_time_out
        self.package.data[CwxMqPoco.KEY_MAX_TIMEOUT] = max_time_out

        self.header.msg_type = CwxMqPoco.MSG_TYPE_CREATE_QUEUE

        return str(self)

    def parse_create_queue_reply(self, msg):
        '''
        @brief parse create队列的reply消息
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return OrderedDict({ret, err})
            ret commit的返回code
            err 失败时的错误消息
        '''
        return self.parse_commit_reply(msg)
    
    def pack_del_queue(self, name, user, passwd, auth_user, auth_passwd):
        '''
        @brief pack delete queue的消息
        @param [in] name 队列的名字
        @param [in] user 队列的用户名
        @param [in] passwd 队列的用户口令
        @param [in] auth_user mq监听的用户名
        @param [in] auth_passwd mq监听的用户口令
        @return 生成的数据包
        '''
        self._reset()
        self.package.data[CwxMqPoco.KEY_NAME] = name
        self.package.data[CwxMqPoco.KEY_USER] = user
        self.package.data[CwxMqPoco.KEY_PASSWD] = passwd
        self.package.data[CwxMqPoco.KEY_AUTH_USER] = auth_user
        self.package.data[CwxMqPoco.KEY_AUTH_PASSWD] = auth_passwd

        self.header.msg_type = CwxMqPoco.MSG_TYPE_DEL_QUEUE

        return str(self)

    def parse_del_queue_reply(self, msg):
        '''
        *@brief parse delete队列的reply消息
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return (ret, err)
            ret commit的返回code
            err 失败时的错误消息
        '''
        return self.parse_commit_reply(msg)

    def parse_sync_data(self, msg):
        '''
        @brief parse mq的sync msg的消息包
        @param [in] msg 接收到的mq消息，不包括msg header。
        @return [OrderedDict({sid, timestamp, data, group, type}),...]
            返回的list中按顺序包含每条消息。非chunk模式只包含一个消息。
            sid 消息的sid。
            timestamp 消息接收时的时间。
            data 消息的data。
            group 消息的group。
            type 消息的type。
        '''
        self.parse_package(msg)
        packages = []
        if len(self.package.data) == 1 and CwxMqPoco.KEY_M in self.package.data:
            msgs = self.package.get_key(CwxMqPoco.KEY_M)
            if isinstance(msgs, list):
                for m in self.package.get_key(CwxMqPoco.KEY_M):
                    p = CwxPackage()
                    p.data = m
                    packages.append(p)
            else:
                p = CwxPackage()
                p.data = msgs
                packages.append(p)
        else:
            packages.append(self.package)
    
        res = []
        for p in packages:
            sid = p.get_key_int(CwxMqPoco.KEY_SID)
            if sid == None:
                raise CwxMqError(CwxMqError.NO_SID,
                            "No key[%s] in recv page." % CwxMqPoco.KEY_SID)
            time_stamp = p.get_key_int(CwxMqPoco.KEY_TIMESTAMP)
            if time_stamp == None:
                raise CwxMqError(CwxMqError.NO_TIMESTAMP, 
                    "No key[%s] in recv page." % CwxMqPoco.KEY_TIMESTAMP)         
            if CwxMqPoco.KEY_DATA in p.data:
                data = p.get_key(CwxMqPoco.KEY_DATA)
            else:
                raise CwxMqError(CwxMqError.NO_KEY_DATA,
                    "No key[%s] in recv page." % CwxMqPoco.KEY_DATA)
            
            group = p.get_key_int(CwxMqPoco.KEY_GROUP) or 0

            res.append(OrderedDict((("sid",sid), ("timestamp",time_stamp), 
                    ("data",data), ("group",group),
                    ("type",self.package.get_key_int(CwxMqPoco.KEY_TYPE)))))
        return res

    def pack_sync_data_reply(self, task_id, sid):
        '''
        @brief pack mq的sync msg的消息包的回复
        @param [in] task_id task-id。
        @param [in] sid 消息的sid。
        @return 生成的数据包
        '''
        self._reset()
        self.package.data[CwxMqPoco.KEY_SID] = sid

        self.header.msg_type = CwxMqPoco.MSG_TYPE_SYNC_DATA_REPLY
        self.header.task_id = task_id

        return str(self)