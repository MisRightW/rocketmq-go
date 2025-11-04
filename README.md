# RocketMQ Python客户端项目

## 项目简介
核心目的：提供RockerMQ SDK在Windows环境中的Python SDK存在的各种兼容性问题
这是一个基于Go语言实现的RocketMQ客户端DLL，配合Python脚本实现高效的消息消费功能。项目采用长连接模式设计，能够稳定地从RocketMQ服务器接收消息并进行处理。

## 项目结构

```
d:\goProjects\rocketmq-go/
├── rocketmq.go          # Go语言实现的RocketMQ SDK 封装 DLL源码
├── go.mod               # Go项目依赖管理
├── go.sum               # Go依赖版本锁定
├── build/rocketmq_consumer.dll    # 编译后的Windows DLL文件
├── build/rocketmq_consumer.h      # DLL头文件
├── example/rocketmq_consumer_example.py  # Python消费者使用示例
└── example/rocketmq_producer_example.py  # Python生产者使用示例
```

## 核心功能

- **长连接消息消费**：与RocketMQ服务器建立持久连接，持续接收消息
- **异步消息处理**：支持异步处理接收到的消息，不阻塞主消费线程
- **完善的错误处理**：包含超时处理、异常捕获和详细日志输出
- **自动资源管理**：在程序退出时自动关闭连接并释放资源
- **跨语言内存优化**：通过Go内部内存管理，避免Python与Go之间的内存释放问题

## 技术栈

- **Go语言**：实现RocketMQ客户端核心逻辑并编译为DLL
- **Python**：调用DLL提供友好的消息消费接口
- **RocketMQ**：分布式消息中间件
- **CGO**：实现Go与C的互操作

## 安装指南

### 前提条件

1. 安装Go 1.16+环境
2. 安装Python 3.6+环境
3. 确保能够访问RocketMQ服务器

### 编译Go DLL

在项目根目录执行以下命令编译生成Windows DLL：

```powershell
go build -buildmode=c-shared -o build/rocketmq_service.dll
```

### 安装依赖

Python端无需额外安装依赖，使用标准库即可：
- ctypes：用于调用DLL函数
- os：用于文件路径和系统操作
- time：用于超时控制和休眠
- traceback：用于异常信息捕获

## 使用说明

### 基本使用流程

1. **初始化消费者**：配置RocketMQ服务器地址、主题、消费者组和标签
2. **开始监听消息**：进入主循环，持续获取并处理消息
3. **处理消息**：对接收到的消息进行业务处理
4. **资源清理**：在程序退出时关闭消费者连接

### 示例代码

```python
from rocketmq_consumer_example import RocketMQConsumer

# 创建消费者实例
consumer = RocketMQConsumer()

# 初始化消费者
initialized = consumer.initialize(
    name_server="192.168.1.2:9876", # RocketMQ服务器地址
    topic="test_task",              # 要订阅的主题
    group="test_task_group",        # 消费者组
    tag="tag"                       # 消息标签
)

# 检查初始化是否成功
if not initialized:
    print("初始化失败，程序退出")
    exit()

# 获取并处理消息
while True:
    message = consumer.get_next_message(timeout_ms=5000)  # 5秒超时
    if message:
        # 处理消息逻辑
        print(f"收到消息: {message}")
    else:
        # 没有消息时短暂休眠
        time.sleep(0.1)

# 程序退出前关闭消费者（通常在finally块中执行）
consumer.shutdown()
```

### 运行长连接模式脚本

直接运行项目中的`rocketmq_consumer_example.py`文件即可启动长连接模式的消息消费：

```powershell
python rocketmq_consumer_example.py
```

## API说明

### RocketMQConsumer类

#### 初始化方法

```python
def __init__(self, dll_path=None):
    # 参数:
    #   dll_path: RocketMQ消费者DLL的路径，默认为当前目录下的rocketmq_consumer.dll
```

#### 加载DLL

```python
def load_dll(self):
    # 返回值:
    #   True: DLL加载成功
    #   False: DLL加载失败
```

#### 初始化消费者连接

```python
def initialize(self, name_server, topic, group, tag):
    # 参数:
    #   name_server: RocketMQ名称服务器地址，格式为"IP:端口"
    #   topic: 要订阅的主题名称
    #   group: 消费者组名称
    #   tag: 消息过滤标签
    # 返回值:
    #   True: 初始化成功
    #   False: 初始化失败
```

#### 获取下一条消息

```python
def get_next_message(self, timeout_ms=5000):
    # 参数:
    #   timeout_ms: 超时时间（毫秒），默认5000ms
    # 返回值:
    #   str: 收到的消息内容
    #   None: 超时或获取消息失败
```

#### 关闭消费者连接

```python
def shutdown(self):
    # 返回值:
    #   True: 关闭成功
    #   False: 关闭失败
```

## 常见问题

### 1. DLL加载失败

- 确保`rocketmq_service.dll`文件存在于指定路径
- 检查当前工作目录是否正确
- 确认系统架构（32位/64位）与DLL匹配

### 2. 连接RocketMQ服务器失败

- 检查`name_server`参数格式是否正确（IP:端口）
- 确认网络是否能够访问RocketMQ服务器
- 验证RocketMQ服务是否正常运行

### 3. 收不到消息

- 检查主题和标签是否正确
- 确认消息生产者是否在发送消息
- 查看消费者组配置是否正确

### 4. 程序异常退出

- 检查Python和Go之间的内存管理是否正常
- 查看程序日志中的错误信息
- 确认RocketMQ服务器连接状态

## 注意事项

1. **性能优化**：在生产环境中，建议根据实际需求调整超时时间和休眠间隔
2. **错误处理**：实现业务逻辑时，请确保正确处理可能出现的异常情况
3. **资源管理**：程序退出前务必调用`shutdown()`方法关闭连接并释放资源
4. **内存管理**：当前版本已优化内存管理机制，由Go内部处理内存释放，无需Python端进行额外操作
5. **日志级别**：可根据需要调整日志输出级别，当前默认输出详细调试信息

## 开发说明

### 修改Go源码后重新编译

每次修改`rockermq.go`后，需要重新编译生成DLL文件：

```powershell
go build -buildmode=c-shared -o build/rocketmq_service.dll
```

### 调试技巧

1. 查看程序输出的调试日志，了解程序运行状态
2. 使用`Ctrl+C`可以中断正在运行的程序
3. 检查RocketMQ服务器的日志，排查连接问题

## License

本项目采用MIT许可证。