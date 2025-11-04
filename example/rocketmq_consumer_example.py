import ctypes
import os
import time
import traceback

class RocketMQConsumer:
    def __init__(self, dll_path=None):
        """初始化RocketMQ消费者"""
        # 获取DLL文件的绝对路径
        if dll_path is None:
            self.dll_path = os.path.abspath('rocketmq_service.dll')
        else:
            self.dll_path = dll_path
        
        # 标志是否已初始化
        self.initialized = False
        self.rocketmq_dll = None
        self.msvcrt_dll = None
        
    def load_dll(self):
        """加载DLL文件"""
        print(f"[调试] 尝试加载DLL: {self.dll_path}")
        print(f"[调试] 当前工作目录: {os.getcwd()}")
        
        # 检查DLL文件是否存在
        if not os.path.exists(self.dll_path):
            print(f"[错误] DLL文件不存在: {self.dll_path}")
            return False
        
        try:
            # 加载RocketMQ消费者DLL
            print("[调试] 开始加载rocketmq_service.dll...")
            self.rocketmq_dll = ctypes.CDLL(self.dll_path)
            print("[调试] rocketmq_service.dll加载成功")
            
            # 定义函数返回类型
            print("[调试] 设置函数返回类型...")
            self.rocketmq_dll.InitializeConsumer.restype = ctypes.c_int
            self.rocketmq_dll.GetNextMessage.restype = ctypes.c_char_p
            self.rocketmq_dll.ShutdownConsumer.restype = ctypes.c_int
            print("[调试] 函数返回类型设置完成")
            
            # 注意：由于Go侧已经优化了内存管理，不再需要Python释放内存
            print("[调试] 内存管理由Go内部处理，无需Python释放")
            
            return True
        except OSError as e:
            print(f"[错误] 无法加载DLL文件: {e}")
            print(f"[错误详情] {traceback.format_exc()}")
            return False
    
    def initialize(self, name_server, topic, group, tag):
        """初始化RocketMQ消费者连接"""
        print("[调试] 开始初始化RocketMQ消费者...")
        
        if self.rocketmq_dll is None:
            print("[调试] DLL未加载，尝试加载...")
            if not self.load_dll():
                return False
        
        try:
            # 将Python字符串转换为C字符串
            print("[调试] 准备参数...")
            name_server_c = ctypes.c_char_p(name_server.encode('utf-8'))
            topic_c = ctypes.c_char_p(topic.encode('utf-8'))
            group_c = ctypes.c_char_p(group.encode('utf-8'))
            tag_c = ctypes.c_char_p(tag.encode('utf-8'))
            
            # 调用DLL中的初始化函数
            print("[调试] 调用InitializeConsumer函数...")
            result = self.rocketmq_dll.InitializeConsumer(
                name_server_c,
                topic_c,
                group_c,
                tag_c
            )
            
            if result == 0:
                print(f"成功初始化RocketMQ消费者")
                print(f"连接服务器: {name_server}")
                print(f"订阅主题: {topic}, 标签: {tag}")
                print(f"消费者组: {group}")
                self.initialized = True
                return True
            elif result == 1:
                print("RocketMQ消费者已经在运行")
                self.initialized = True
                return True
            else:
                error_messages = {
                    -1: "创建消费者失败",
                    -2: "订阅主题失败",
                    -3: "启动消费者失败"
                }
                error_msg = error_messages.get(result, f"未知错误 (代码: {result})")
                print(f"初始化RocketMQ消费者失败: {error_msg}")
                return False
        except Exception as e:
            print(f"初始化过程中发生错误: {e}")
            return False
    
    def get_next_message(self, timeout_ms=5000):
        """获取下一条消息，超时返回None"""
        print(f"[调试] 尝试获取下一条消息 (超时: {timeout_ms}毫秒)")
        
        if not self.initialized or self.rocketmq_dll is None:
            print("[错误] 消费者尚未初始化")
            return None
        
        try:
            # 调用DLL中的获取消息函数
            print("[调试] 调用GetNextMessage函数...")
            result_ptr = self.rocketmq_dll.GetNextMessage(timeout_ms)
            
            # 检查返回值是否为NULL
            if result_ptr is None:
                print("[警告] 调用DLL函数返回NULL")
                return None
            
            # 将C字符串转换为Python字符串
            result = ctypes.string_at(result_ptr).decode('utf-8')
            print(f"[调试] 获取到原始消息: {result}")
            
            # 处理特殊返回值
            if result == "TIMEOUT":
                print("[调试] 获取消息超时")
                return None
            elif result == "EMPTY_MESSAGE":
                print("[调试] 接收到空消息")
                return None
            # 如果返回错误信息
            elif result.startswith("Error:"):
                print(f"[错误] 获取消息失败: {result}")
                return None
            
            print("[调试] 成功获取有效消息")
            return result
            
            # 处理特殊返回值
            if result == "TIMEOUT":
                print("[调试] 获取消息超时")
                return None
            elif result == "EMPTY_MESSAGE":
                print("[调试] 接收到空消息")
                return None
            # 如果返回错误信息
            elif result.startswith("Error:"):
                print(f"[错误] 获取消息失败: {result}")
                return None
            
            print("[调试] 成功获取有效消息")
            return result
        except Exception as e:
            print(f"[错误] 获取消息时发生错误: {e}")
            print(f"[错误详情] {traceback.format_exc()}")
            return None
    
    def shutdown(self):
        """关闭消费者连接并释放资源"""
        print("[调试] 开始关闭RocketMQ消费者...")
        
        if not self.initialized or self.rocketmq_dll is None:
            print("[信息] 消费者未初始化，无需关闭")
            return True
        
        try:
            # 调用DLL中的关闭函数
            print("[调试] 调用ShutdownConsumer函数...")
            result = self.rocketmq_dll.ShutdownConsumer()
            
            if result == 0:
                print("[成功] 成功关闭RocketMQ消费者")
                self.initialized = False
                return True
            else:
                print(f"[错误] 关闭RocketMQ消费者失败 (代码: {result})")
                return False
        except Exception as e:
            print(f"[错误] 关闭过程中发生错误: {e}")
            print(f"[错误详情] {traceback.format_exc()}")
            return False


def process_message(message):
    """处理接收到的消息"""
    try:
        print(f"\n[消息处理]")
        print(f"收到消息内容: {message}")
        # 这里可以添加具体的业务逻辑
        
            
    except Exception as e:
        print(f"处理消息时发生错误: {e}")


def main():
    print("===== RocketMQ Python客户端 (长连接模式) =====")
    print(f"[系统信息] Python版本: {os.sys.version}")
    print(f"[系统信息] 操作系统: {os.name}")
    print(f"[系统信息] 当前进程ID: {os.getpid()}")
    
    # 创建消费者实例
    print("[调试] 创建RocketMQConsumer实例...")
    consumer = RocketMQConsumer()
    
    try:
        # 初始化消费者
        print("[调试] 开始初始化消费者...")
        initialized = consumer.initialize(
            name_server="192.168.1.2:9876",
            topic="test_task",
            group="test_task_group",
            tag="tag"
        )
        
        if not initialized:
            print("[错误] 初始化失败，程序退出")
            return
        
        print("\n[状态] 开始持续监听消息...")
        print("[提示] 按 Ctrl+C 可随时终止程序")
        print("====================================")
        
        # 记录循环次数，用于调试
        loop_count = 0
        
        # 持续监听消息的主循环
        while True:
            loop_count += 1
            print(f"[循环 {loop_count}] 等待新消息...")
            
            # 获取下一条消息（5秒超时）
            message = consumer.get_next_message(timeout_ms=5000)
            
            if message:
                # 处理收到的消息
                print("[状态] 接收到消息，开始处理...")
                process_message(message)
            else:
                # 没有收到消息，短暂休眠以降低CPU使用率
                print("[状态] 未收到新消息")
                time.sleep(0.1)
                
    except KeyboardInterrupt:
        print("\n====================================")
        print("[状态] 程序已被用户中断")
    except Exception as e:
        print(f"\n[严重错误] 程序发生未预期错误: {e}")
        print(f"[错误详情] {traceback.format_exc()}")
    finally:
        # 确保在程序退出前关闭消费者连接
        print("\n[清理] 正在清理资源...")
        consumer.shutdown()
        print("[完成] 程序已安全退出")


if __name__ == "__main__":
    main()