"""
This is the test dispatcher.

It will dispatch tests against any registered test runners when the repo
observer sends it a 'dispatch' message with the commit ID to be used. It
will store results when the test runners have completed running the tests and
send back the results in a 'results' messagee

It can register as many test runners as you like. To register a test runner,
be sure the dispatcher is started, then start the test runner.

这是测试调度器。

当仓库观察者发送包含 commit ID 的 'dispatch' 消息时，它会将测试分发给已注册的测试执行器。
当测试执行器完成测试并返回 'results' 消息时，它会存储测试结果。

你可以注册任意数量的测试执行器。要注册测试执行器，请先启动调度器，然后启动测试执行器。
ThreadingTCPServer (server)
├── runners: [                    # 已注册的 runner 列表
│     {"host": "localhost", "port": "8900"},
│     {"host": "localhost", "port": "8901"}
│   ]
├── dispatched_commits: {         # 正在执行的测试
│     "abc123": {"host": "localhost", "port": "8900"},
│     "def456": {"host": "localhost", "port": "8901"}
│   }
├── pending_commits: [            # 等待重分配的测试
│     "789xyz"
│   ]
└── dead: False                   # 服务器是否关闭
"""
import argparse
import os
import re
import socket
import socketserver
import time
import threading

import helpers

# ==================== 核心函数：分发测试任务 ====================
# Shared dispatcher code
def dispatch_tests(server, commit_id):
    """
    将测试任务分发给可用的 test runner
    
    参数:
        server: ThreadingTCPServer 实例，包含 runners 列表
        commit_id: 需要测试的提交 ID
    
    工作流程:
        1. 遍历所有已注册的 runner
        2. 依次询问每个 runner 是否能执行测试
        3. 如果某个 runner 响应 "OK"，分配任务给它
        4. 如果没有可用 runner，等待2秒后重试
    
    这个函数会一直尝试，直到成功分配为止
    """
    # NOTE: usually we don't run this forever
    # 无限循环，直到找到可用的 runner
    while True:
        print("trying to dispatch to runners")

        # 遍历所有已注册的测试执行器
        for runner in server.runners:
            # 向 runner 发送 runtest 命令，附带 commit ID
            response = helpers.communicate(runner["host"],
                                           int(runner["port"]),
                                           "runtest:%s" % commit_id)
            
            # 如果 runner 接受了任务
            if response == "OK":
                print("adding id %s" % commit_id)
                # 记录这个 commit 由哪个 runner 执行
                server.dispatched_commits[commit_id] = runner

                # 如果这个 commit 在待处理列表中，移除它
                if commit_id in server.pending_commits:
                    server.pending_commits.remove(commit_id)
                return
       
        # 没有可用的 runner，等待2秒后重试
        time.sleep(2)


# ==================== 多线程 TCP 服务器类 ====================
class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """
    支持多线程的 TCP 服务器
    
    同时继承:
    - ThreadingMixIn: 让每个请求在新线程中处理（并发）
    - TCPServer: 提供 TCP 服务器基础功能
    
    额外属性（用于存储调度器的状态）:
    - runners: 已注册的测试执行器列表 [{"host":..., "port":...}, ...]
    - dead: 服务器是否已关闭的标记（用于通知后台线程退出）
    - dispatched_commits: 正在执行的 {commit_id: runner} 映射
    - pending_commits: 等待重新分配的 commit 列表（当 runner 失败时）
    """
    runners = [] # Keeps track of test runner pool
    dead = False # Indicate to other threads that we are no longer running
    dispatched_commits = {} # Keeps track of commits we dispatched
    pending_commits = [] # Keeps track of commits we have yet to dispatch

# ==================== 请求处理器 ====================
class DispatcherHandler(socketserver.BaseRequestHandler):
    """
    调度器的请求处理器
    
    处理来自以下地方的请求:
    - 仓库观察者: status（检查状态）, dispatch（分发测试）
    - 测试执行器: register（注册）, results（返回结果）
    """

    command_re = re.compile(r"(\w+)(:.+)*")
    BUF_SIZE = 1024

    def handle(self):
        """
        处理每个连接请求的主方法
        当客户端连接时自动调用
        """

        # self.request is the TCP socket connected to the client
        raw_data = self.request.recv(self.BUF_SIZE).strip()
        try:
            self.data = raw_data.decode()
        except UnicodeDecodeError:
            self.request.sendall(b"Invalid encoding")
            return
        
         # 解析命令格式: "命令:参数"
        command_groups = self.command_re.match(self.data)
        if not command_groups:
            self.request.sendall(b"Invalid command")
            return
        command = command_groups.group(1)
        
        # ========== 1. 状态检查命令 ==========
        if  command == "status":
            """仓库观察者或执行器用来检查调度器是否存活"""
            print("in status")
            self.request.sendall(b"OK")
        # ========== 2. 注册命令 ==========
        elif command == "register":
            """测试执行器启动时调用，向调度器注册自己"""

            print("registering runner")
            # 获取地址参数，格式如 ":localhost:8900"
            address = command_groups.group(2)
            if address and address.startswith(":"): # 移除开头的冒号
                address = address[1:]
            # Parse host and port
            parts = address.split(":")
            if len(parts) == 2:
                host, port = parts
            else:
                # Fallback to regex
                host, port = re.findall(r"(\w+)", address)

            # 创建 runner 信息字典并添加到池中
            runner = {"host": host, "port": port}
            self.server.runners.append(runner)
            print("Runner registered: %s:%s" % (host, port))
            self.request.sendall(b"OK")
        # ========== 3. 分发命令 ==========
        elif command == "dispatch":
            """仓库观察者调用，通知有新的提交需要测试"""
            print("going to dispatch")
            commit_id = command_groups.group(2)
            # 获取 commit_id，格式如 ":abc123def"
            if commit_id and commit_id.startswith(":"):
                commit_id = commit_id[1:]

            # 检查是否有已注册的 runner
            if not self.server.runners:
                self.request.sendall(b"No runners are registered")
            else:
                # 确认收到请求，然后分发测试
                self.request.sendall(b"OK")
                dispatch_tests(self.server, commit_id)
        # ========== 4. 结果命令 ==========
        elif command == "results":
            """测试执行器调用，返回测试结果"""
            print("got test results")

            # 获取结果参数，格式如 ":commit_id:长度:结果数据"
            results = command_groups.group(2)
            if results and results.startswith(":"):
                results = results[1:]
            results = results.split(":")
            commit_id = results[0]
            length_msg = int(results[1])

            
            # 3 is the number of ":" in the sent command
            # 计算还需要接收多少数据
            remaining_buffer = self.BUF_SIZE - (len(command) + len(commit_id) + len(results[1]) + 3)
            
            # 如果结果数据超过缓冲区，继续接收剩余数据
            if length_msg > remaining_buffer:
                more_data = self.request.recv(length_msg - remaining_buffer).strip()
                if isinstance(more_data, bytes):
                    more_data = more_data.decode()
                self.data += more_data

            # 从已分发列表中移除这个 commit ID
            if commit_id in self.server.dispatched_commits:
                del self.server.dispatched_commits[commit_id]
            
            # 创建结果目录（如果不存在）
            if not os.path.exists("test_results"):
                os.makedirs("test_results")

             # 将测试结果保存到文件（文件名是 commit_id）
            with open("test_results/%s" % commit_id, "w") as f:
                # 提取结果数据部分（第4个冒号之后的内容）
                data_parts = self.data.split(":")[3:]
                data = "\n".join(data_parts)
                f.write(data)
            self.request.sendall(b"OK") 
        else:
            self.request.sendall(b"Invalid command")

# ==================== 主服务函数 ====================
def serve():
    """
    启动调度器服务器
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--host",
                        help="dispatcher's host, by default it uses localhost",
                        default="localhost",
                        action="store")
    parser.add_argument("--port",
                        help="dispatcher's port, by default it uses 8888",
                        default=8888,
                        action="store")
    args = parser.parse_args()

    # Create the server
    server = ThreadingTCPServer((args.host, int(args.port)), DispatcherHandler)
    print('serving on %s:%s' % (args.host, int(args.port)))
    
    # Create a thread to check the runner pool
    def runner_checker(server):
        """
        定期检查每个 runner 是否还活着
        如果 runner 无响应，将其任务重新分配
        """
        def manage_commit_lists(runner):
            """
            处理失败的 runner：
            1. 将该 runner 正在执行的 commit 移到待处理列表
            2. 从 runner 池中移除该 runner
            """
            # 遍历所有已分发的 commit
            for commit, assigned_runner in list(server.dispatched_commits.items()):
                if assigned_runner == runner:
                    # 这个 commit 需要重新分配
                    del server.dispatched_commits[commit]
                    server.pending_commits.append(commit)
                    break
            # 从 runner 池中移除
            if runner in server.runners:
                server.runners.remove(runner)

        # 持续运行直到服务器关闭
        while not server.dead:
            time.sleep(1) # 每秒检查一次 runner 状态
            # 遍历 runner 池的副本（避免遍历时修改）
            for runner in server.runners[:]:  # iterate over a copy
                try:
                    # 发送 ping 命令检查 runner 是否存活
                    response = helpers.communicate(runner["host"],
                                                   int(runner["port"]),
                                                   "ping")
                    if response != "pong":
                        print("removing runner %s" % runner)
                        manage_commit_lists(runner)
                # 如果通信失败，说明 runner 已死，进行清理
                except (socket.error, ConnectionRefusedError) as e:
                    print("Runner error: %s, removing" % e)
                    manage_commit_lists(runner)

    # 重新分配失败的任务
    def redistribute(server):
        """
        重新分配因 runner 失败而未完成的测试任务
        """
        while not server.dead:
            # 遍历待处理列表的副本
            for commit in server.pending_commits[:]:  # iterate over a copy
                print("running redistribute")
                print(server.pending_commits)
                # 尝试重新分发这个 commit
                dispatch_tests(server, commit)
                time.sleep(5) 

    # 创建并启动两个后台线程
    runner_heartbeat = threading.Thread(target=runner_checker, args=(server,))
    redistributor = threading.Thread(target=redistribute, args=(server,))
    try:
        runner_heartbeat.start()
        redistributor.start()
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl+C or Cmd+C
        server.serve_forever()
    except (KeyboardInterrupt, Exception):
        # if any exception occurs, kill the thread
        server.dead = True # 通知后台线程退出
        runner_heartbeat.join() # 等待健康检查线程结束
        redistributor.join() # 等待重分配线程结束


if __name__ == "__main__":
    serve()