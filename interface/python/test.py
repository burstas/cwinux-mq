#coding:utf-8
from cwxmq.CwxMqPoco import CwxMqPoco
import socket
from cwxmq.CwxMsgHeader import CwxMsgHeader
import zlib
import binascii

host = '127.0.0.1'

queue_info = {"port":9906, "name":"my_python_interface_test", "user":"python_user", 
            "passwd":"python_user_passwd", "scribe":"", "auth_user":"mq_admin", 
            "auth_passwd":"mq_admin_passwd"}

send_info = {"port":9901, "data":{"a":1,"b":2, "test":[4,5,{"k1":1,"k2":[6,7]}]},
            "group":112233, "type":4455, "user":"recv", "passwd":"recv_passwd", 
            "attr":0, "sign":"md5", "zip":True}

sync_info = {"port":9903, "user":"async", "passwd":"async_passwd"}

queue_conn = socket.create_connection((host, queue_info["port"]))
send_conn = socket.create_connection((host, send_info["port"]))

poco = CwxMqPoco()

def recv_msg(conn):
    poco.parse_header(conn.recv(CwxMsgHeader.HEAD_LEN))
    msg = conn.recv(poco.header.data_len)
    if poco.header.is_compressed():
        msg = zlib.decompress(msg)
    return msg

def test_create_queue ():
    conn = queue_conn
    pack = poco.pack_create_queue(queue_info["name"], queue_info["user"],
            queue_info["passwd"], queue_info["scribe"], 
            queue_info["auth_user"], queue_info["auth_passwd"], 0, 1)
    conn.sendall(pack)
    msg = recv_msg(conn)
    res = poco.parse_create_queue_reply(msg)
    print "create queue reply:", res
    print
    return res

def test_send_data():
    conn = send_conn
    pack = poco.pack_mq(0, send_info["data"], send_info["group"], 
            send_info["type"], send_info["attr"], send_info["user"], 
            send_info["passwd"], send_info["sign"], send_info["zip"])
    conn.sendall(pack)
    msg = recv_msg(conn)
    res = poco.parse_mq_reply(msg)
    print "send data reply:", res
    print
    return res

def test_fetch_data():
    conn = queue_conn
    pack = poco.pack_fetch_mq(0, queue_info["name"], queue_info["user"], queue_info["passwd"])
    conn.sendall(pack)
    msg = recv_msg(conn)
    res = poco.parse_fetch_mq_reply(msg)
    print "fetch data reply:", res
    print
    return res

def test_del_queue():
    conn = queue_conn
    pack = poco.pack_del_queue(queue_info["name"], queue_info["user"],
            queue_info["passwd"], queue_info["auth_user"], 
            queue_info["auth_passwd"])
    conn.sendall(pack)
    msg = recv_msg(conn)
    res = poco.parse_del_queue_reply(msg)
    print "del queue reply:", res
    print
    return res

def test_commit():
    conn = send_conn
    pack = poco.pack_commit(0, send_info["user"],
            send_info["passwd"])
    conn.sendall(pack)
    msg = recv_msg(conn)
    res = poco.parse_commit_reply(msg)
    print "commit reply:", res
    print
    return res

def test_fetch_commit():
    conn = queue_conn
    pack = poco.pack_fetch_mq_commit(1,0)
    conn.sendall(pack)
    msg = recv_msg(conn)
    res = poco.parse_fetch_mq_commit_reply(msg)
    print "fetch commit reply:", res
    print
    return res

def test_sync_data():
    print "sync data"
    conn = socket.create_connection((host, sync_info["port"]))
    conn.settimeout(2)
    pack = poco.pack_sync_report(0, 209276200, 1, "112233:4455", sync_info["user"], sync_info["passwd"], "crc32", True)
    conn.sendall(pack)
    i = 0
    while i<5:
        i += 1
        try:
            msg = recv_msg(conn)
        except socket.timeout:
            break
        if poco.header.msg_type == CwxMqPoco.MSG_TYPE_SYNC_DATA:
            res = poco.parse_sync_data(msg)
            print "received sync data:"
            for m in res:
                print "\t", m
            pack = poco.pack_sync_data_reply(0, res[-1]["sid"])
            conn.sendall(pack)
        else:
            if poco.header.msg_type == CwxMqPoco.MSG_TYPE_SYNC_REPORT_REPLY:
                print "sync report reply:", poco.parse_sync_report_reply(msg)
                break
            else:
                print "not expected response"

    print

if __name__ == "__main__":
    test_create_queue()
    test_send_data()
    test_commit()

    test_fetch_data()
    test_fetch_commit()
    test_fetch_data()
    
    test_sync_data()

    test_del_queue()