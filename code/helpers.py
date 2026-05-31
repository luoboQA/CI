import socket

def communicate(host, port, request):
    """
    通过网络发送请求并接收响应
    
    参数:
        host: 目标服务器的主机名或IP地址 (例如: 'localhost' 或 '127.0.0.1')
        port: 目标服务器的端口号 (例如: 8888)
        request: 要发送的请求字符串 (例如: 'status' 或 'dispatch:abc123')
    
    返回:
        服务器响应的字符串 (例如: 'OK' 或 'pong')
    """
    # 创建一个 TCP socket
    # socket.AF_INET: 使用 IPv4 地址族
    # socket.SOCK_STREAM: 使用 TCP 协议（面向连接、可靠传输）
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 连接到指定的服务器地址和端口
    s.connect((host, port))

    # 如果 request 是字符串类型，将其编码为字节串
    if isinstance(request, str):
        request = request.encode()
    # 发送请求数据到服务器
    s.send(request)
    # 接收服务器的响应数据
    response = s.recv(1024)
    
    s.close()
    # 将接收到的字节串解码为字符串并返回
    # 例如: b'OK' -> 'OK'
    return response.decode()