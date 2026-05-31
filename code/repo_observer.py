"""
This is the repo observer.

It checks for new commits to the master repo, and will notify the dispatcher of
the latest commit ID, so the dispatcher can dispatch the tests against this
commit ID.
"""
import argparse
import os
import socket
import subprocess
import time

import helpers


def poll():
    """
    主轮询函数 - 无限循环检查仓库变化
    """
    # ========== 1. 解析命令行参数 ==========
    parser = argparse.ArgumentParser()
    parser.add_argument("--dispatcher-server",
                        help="dispatcher host:port, by default it uses localhost:8888",
                        default="localhost:8888",
                        action="store")
    parser.add_argument("repo", metavar="REPO", type=str,
                        help="path to the repository this will observe")
    args = parser.parse_args()

    # 解析调度器地址
    # 例如 "localhost:8888" -> dispatcher_host="localhost", dispatcher_port="8888"
    dispatcher_host, dispatcher_port = args.dispatcher_server.split(":")
    
    # ========== 2. 无限循环，定期检查 ==========
    while True:
        try:
            # call the bash script that will update the repo and check
            # for changes. If there's a change, it will drop a .commit_id file
            # with the latest commit in the current working directory

            # 调用 update_repo.sh 脚本,check_output()是 Python 执行系统命令并获取输出的标准方法
            # 这个脚本会:
            #   1. 进入仓库目录
            #   2. 获取当前 commit ID
            #   3. 执行 git pull
            #   4. 获取新的 commit ID
            #   5. 如果 ID 变化，创建 .commit_id 文件
            subprocess.check_output(["./update_repo.sh", args.repo], stderr=subprocess.STDOUT)# 合并错误输出
        except subprocess.CalledProcessError as e:
            raise Exception("Could not update and check repository. Reason: %s" % e.output.decode())

        # ---------- 步骤 B: 检查是否有新提交 ----------
        # .commit_id 文件存在表示有新提交
        if os.path.isfile(".commit_id"):
            # great, we have a change! let's execute the tests
            # First, check the status of the dispatcher server to see
            # if we can send the tests

            # ---------- 步骤 C: 检查调度器是否存活 ----------
            try:
                 # 发送 "status" 命令给调度器，检查它是否在线
                response = helpers.communicate(dispatcher_host,
                                               int(dispatcher_port),
                                               "status")
            except socket.error as e:
                raise Exception("Could not communicate with dispatcher server: %s" % e)
            
            # ---------- 步骤 D: 如果调度器正常，发送测试任务 ----------
            if response == "OK":
                # Dispatcher is present, let's send it a test
                commit = ""
                # 读取新提交的 commit ID
                with open(".commit_id", "r") as f:
                    commit = f.readline().strip()
                # 发送 "dispatch" 命令给调度器，附带 commit ID
                response = helpers.communicate(dispatcher_host,
                                               int(dispatcher_port),
                                               "dispatch:%s" % commit)
                if response != "OK":
                    raise Exception("Could not dispatch the test: %s" % response)
                print("dispatched!")
            else:
                # Something wrong happened to the dispatcher
                raise Exception("Could not dispatch the test: %s" % response)
        # ---------- 步骤 E: 等待5秒后再次检查 ----------
        time.sleep(5)


if __name__ == "__main__":
    poll()