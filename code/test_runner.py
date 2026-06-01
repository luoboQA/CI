"""
This is the test runner.

It registers itself with the dispatcher when it first starts up, and then waits
for notification from the dispatcher. When the dispatcher sends it a 'runtest'
command with a commit id, it updates its repository clone and checks out the
given commit. It will then run tests against this version and will send back the
results to the dispatcher. It will then wait for further instruction from the
dispatcher.

这是测试执行器。

启动时向调度器注册自己，然后等待调度器的通知。
当调度器发送带 commit ID 的 'runtest' 命令时，它会更新仓库克隆并切换到指定提交。
然后针对这个版本运行测试，并将结果返回给调度器。
之后等待调度器的下一个指令。

python3 test_runner.py
        │
        ▼
┌─────────────────────────────────┐
│ 解释器读取文件，从第1行开始       │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│ 导入所有模块 (import ...)        │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│ 定义类 ThreadingTCPServer        │ ← 只是记录，不执行方法
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│ 定义类 TestHandler               │ ← 只是记录，不执行方法
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│ 定义函数 serve()                 │ ← 只是记录，不执行函数体
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│ 执行 if __name__ == "__main__"  │
└─────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────┐
│ 调用 serve()                    │ ← 真正"业务逻辑"
│   ├─ 解析参数                    │
│   ├─ 创建服务器                  │
│   ├─ 注册到调度器                │
│   └─ server.serve_forever()     │ ← 阻塞监听
└─────────────────────────────────┘

t = threading.Thread(target=dispatcher_checker, args=(server,))  
t.start() # 启动检查线程
检查线程: [启动] ──► [sleep 5s] ──► [检查] ──► [sleep 5s] ──► [检查] ──► ...

server.serve_forever()
    ↓
不断循环接受连接
    ↓
收到客户端连接
    ↓
创建新的线程处理请求
    ↓
调用 TestHandler 的方法


"""
import argparse
import errno
import os
import re
import socket
import socketserver
import subprocess
import time
import threading
import unittest

import helpers

# ==================== 多线程 TCP 服务器 ====================
class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """
    支持多线程的 TCP 服务器
    
    额外属性:
    - dispatcher_server: 调度器的地址信息 {"host":..., "port":...}
    - last_communication: 最后一次收到调度器消息的时间
    - busy: 是否正在执行测试任务
    - dead: 服务器是否已关闭
    """
    dispatcher_server = None 
    last_communication = None 
    busy = False
    dead = False

# ==================== 请求处理器 ====================
class TestHandler(socketserver.BaseRequestHandler):
    """
    测试执行器的请求处理器,处理每个收到的消息(来自调度器)
    
    处理来自调度器的命令:
    - ping: 心跳检测
    - runtest: 执行测试任务
    """
    
    command_re = re.compile(r"(\w+)(:.+)*") # 正则匹配命令格式
    """
    (\w+) - 第一个捕获组：匹配一个或多个单词字符（字母、数字、下划线）

    (:.+)* - 第二个捕获组，重复0次或多次：
    :  - 匹配冒号字面量

    .+ - 匹配一个或多个任意字符（除换行符外）
    """
    def handle(self):
        """
        处理每个连接请求的主方法
        当调度器连接时自动调用
        """
        # self.request 是连接到调度器的 TCP socket

        # 从 socket 接收数据并解码为字符串
        raw_data = self.request.recv(1024).strip()
        try:
            self.data = raw_data.decode()
        except UnicodeDecodeError:
            self.request.sendall(b"Invalid encoding")
            return
        
        # 使用正则表达式解析命令，从字符串开头尝试匹配该正则表达式
        command_groups = self.command_re.match(self.data)
        if not command_groups:
            self.request.sendall(b"Invalid command")
            return
        command = command_groups.group(1) #返回第一个捕获组 (\w+) 匹配到的内容
        
        # ========== 1. Ping 命令（心跳检测） ==========
        if command == "ping":
            print("pinged")
            # 更新最后通信时间
            self.server.last_communication = time.time()
            # 回复 pong
            self.request.sendall(b"pong")
        # ========== 2. 运行测试命令 ==========
        elif command == "runtest":
            """"调度器要求执行测试任务"""
            print("got runtest command: am I busy? %s" % self.server.busy)
            if self.server.busy:
                self.request.sendall(b"BUSY")
            else:
                # 接受任务
                self.request.sendall(b"OK")
                print("running")
                # 提取 commit_id（格式如 ":abc123def"）
                commit_id = command_groups.group(2)
                if commit_id and commit_id.startswith(":"):
                    commit_id = commit_id[1:] # 移除开头的冒号
                self.server.busy = True
                # 执行测试
                self.run_tests(commit_id, self.server.repo_folder)
                # 完成，标记为空闲
                self.server.busy = False
        else:
            self.request.sendall(b"Invalid command")

    def run_tests(self, commit_id, repo_folder):
        """
        测试指定 commit ID 的代码，并将结果发送给调度器
        
        步骤:
        1. 更新仓库到指定 commit
        2. 运行 unittest 发现并执行测试
        3. 将结果发送给调度器
        """
        # ========== 步骤1: 更新仓库到指定 commit ==========
        try:
            # 调用 shell 脚本更新仓库
            output = subprocess.check_output(["./test_runner_script.sh",
                                            repo_folder, commit_id],# 第1个参数：仓库文件夹路径, 第2个参数：commit ID
                                            stderr=subprocess.STDOUT) # 将标准错误（stderr）重定向到标准输出（stdout）
            print(output.decode()) # 打印脚本输出
        except subprocess.CalledProcessError as e:
            # 更新失败，打印错误并返回
            print("Error updating repo: %s" % e.output.decode())
            return
        
        # ========== 步骤2: 运行单元测试 ==========
        # 构建测试文件夹路径
        test_folder = os.path.join(repo_folder, "tests")
        # 使用 unittest 自动发现并加载所有测试
        # discover() 会查找 test_folder 下所有 test_*.py 文件
        suite = unittest.TestLoader().discover(test_folder)

        # 打开结果文件
        result_file = open("results", "w")
        # 运行测试，将结果写入文件
        # TextTestRunner 会输出类似 "F." 的进度和详细错误信息
        unittest.TextTestRunner(result_file).run(suite)
        result_file.close()
        
        # ========== 步骤3: 将结果发送给调度器 ==========
        # 格式: results:commit_id:数据长度:结果数据
        with open("results", "r") as result_file:
            output = result_file.read()
        helpers.communicate(self.server.dispatcher_server["host"],
                            int(self.server.dispatcher_server["port"]),
                            "results:%s:%s:%s" % (commit_id, len(output), output))

# ==================== 主服务函数 ====================
def serve():
    """
    启动测试执行器的主函数
    """
    range_start = 8900 # 自动分配端口的起始值

    # 解析命令行参数
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",
                        help="runner's host, by default it uses localhost",
                        default="localhost",
                        action="store")
    parser.add_argument("--port",
                        help="runner's port, by default it uses values >=%s" % range_start,
                        action="store")
    parser.add_argument("--dispatcher-server",
                        help="dispatcher host:port, by default it uses localhost:8888",
                        default="localhost:8888",
                        action="store")
    parser.add_argument("repo", metavar="REPO", type=str,
                        help="path to the repository this will observe")
    args = parser.parse_args()

    runner_host = args.host
    runner_port = None
    tries = 0 # 已尝试的端口数量
        
    # ========== 分配端口 ==========
    if not args.port:
        # 自动分配端口：从 8900 开始尝试
        runner_port = range_start
        while tries < 100:
            try:
                # 尝试绑定端口
                server = ThreadingTCPServer((runner_host, runner_port), TestHandler) # 创建 TCP 服务器实例
                print(server)
                print(runner_port)
                break # 绑定成功，退出循环
            except socket.error as e: # 端口已被占用
                if e.errno == errno.EADDRINUSE:
                    tries += 1
                    runner_port = runner_port + tries # 尝试下一个端口
                    continue
                else:
                    raise e
        else:
            raise Exception("Could not bind to ports in range %s-%s" % (range_start, range_start+tries))
    else:
        # 用户指定了端口
        runner_port = int(args.port)
        server = ThreadingTCPServer((runner_host, runner_port), TestHandler)
    
    # 设置仓库文件夹路径
    server.repo_folder = args.repo

    # ========== 向调度器注册 ==========
    # 1. 解析调度器地址
    # args.dispatcher_server 格式是 "localhost:8888" 或 "192.168.1.100:8888"
    dispatcher_host, dispatcher_port = args.dispatcher_server.split(":")
    # 2. 保存调度器信息到调度器服务器对象
    server.dispatcher_server = {"host": dispatcher_host, "port": dispatcher_port}
    
    print("Registering with dispatcher at %s:%s" % (dispatcher_host, dispatcher_port))

    # 3. 向调度器发送注册请求
    response = helpers.communicate(server.dispatcher_server["host"],
                                   int(server.dispatcher_server["port"]),
                                   "register:%s:%s" % (runner_host, runner_port))
    print("Registration response: '%s'" % response)
    
    if response != "OK":
        raise Exception("Can't register with dispatcher!")

     # ========== 调度器检查线程 ==========
    def dispatcher_checker(server):
        """
        定期检查调度器是否还活着
        如果调度器挂了，执行器也自动关闭
        """
        while not server.dead:
            time.sleep(5) # 每5秒检查一次

            # 如果超过10秒没收到调度器消息
            if server.last_communication and (time.time() - server.last_communication) > 10:
                try:
                    # 尝试 ping 调度器
                    response = helpers.communicate(server.dispatcher_server["host"],
                                                   int(server.dispatcher_server["port"]),
                                                   "status")
                    if response != "OK":
                        print("Dispatcher is no longer functional")
                        server.shutdown()
                        return
                except socket.error as e:
                    print("Can't communicate with dispatcher: %s" % e)
                    server.shutdown()
                    return
                
    # # 创建一个线程，运行 dispatcher_checker 函数,args=(server,): 传递给函数的参数（注意逗号，表示元组）
    t = threading.Thread(target=dispatcher_checker, args=(server,))
    try:
        t.start()
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever() #开始监听并处理请求,既是服务器(ThreadingTCPServer)又是执行器(TestHandler) 
    except (KeyboardInterrupt, Exception):
        # if any exception occurs, kill the thread
        server.dead = True
        t.join() # join() 会阻塞直到线程 t 执行完毕


if __name__ == "__main__":
    serve()