import ctypes
import os
import time
import traceback

class RocketMQProducer:
    def __init__(self, dll_path=None):
        """初始化RocketMQ生产者"""
        # 获取DLL文件的绝对路径
        if dll_path is None:
            self.dll_path = os.path.abspath('rocketmq_service.dll')
        else:
            self.dll_path = dll_path
        
        # 标志是否已初始化
        self.initialized = False
        self.rocketmq_dll = None
        
    def load_dll(self):
        """加载DLL文件"""
        print(f"[调试] 尝试加载DLL: {self.dll_path}")
        print(f"[调试] 当前工作目录: {os.getcwd()}")
        
        # 检查DLL文件是否存在
        if not os.path.exists(self.dll_path):
            print(f"[错误] DLL文件不存在: {self.dll_path}")
            return False
        
        try:
            # 加载RocketMQ生产者DLL
            print("[调试] 开始加载rocketmq_service.dll...")
            self.rocketmq_dll = ctypes.CDLL(self.dll_path)
            print("[调试] rocketmq_service.dll加载成功")
            
            # 定义函数返回类型
            print("[调试] 设置函数返回类型...")
            self.rocketmq_dll.InitializeProducer.restype = ctypes.c_int
            self.rocketmq_dll.SendMessage.restype = ctypes.c_char_p
            self.rocketmq_dll.ShutdownProducer.restype = ctypes.c_int
            print("[调试] 函数返回类型设置完成")
            
            print("[调试] 内存管理由Go内部处理，无需Python释放")
            
            return True
        except OSError as e:
            print(f"[错误] 无法加载DLL文件: {e}")
            print(f"[错误详情] {traceback.format_exc()}")
            return False
    
    def initialize(self, name_server, group):
        """初始化RocketMQ生产者连接"""
        print("[调试] 开始初始化RocketMQ生产者...")
        
        if self.rocketmq_dll is None:
            print("[调试] DLL未加载，尝试加载...")
            if not self.load_dll():
                return False
        
        try:
            # 将Python字符串转换为C字符串
            print("[调试] 准备参数...")
            name_server_c = ctypes.c_char_p(name_server.encode('utf-8'))
            group_c = ctypes.c_char_p(group.encode('utf-8'))
            
            # 调用DLL中的初始化函数
            print("[调试] 调用InitializeProducer函数...")
            result = self.rocketmq_dll.InitializeProducer(
                name_server_c,
                group_c
            )
            
            if result == 0:
                print(f"成功初始化RocketMQ生产者")
                print(f"连接服务器: {name_server}")
                print(f"生产者组: {group}")
                self.initialized = True
                return True
            elif result == 1:
                print("RocketMQ生产者已经在运行")
                self.initialized = True
                return True
            else:
                error_messages = {
                    -1: "创建生产者失败",
                    -2: "启动生产者失败",
                    -4: "参数无效"
                }
                error_msg = error_messages.get(result, f"未知错误 (代码: {result})")
                print(f"初始化RocketMQ生产者失败: {error_msg}")
                return False
        except Exception as e:
            print(f"初始化过程中发生错误: {e}")
            print(f"[错误详情] {traceback.format_exc()}")
            return False
    
    def send_message(self, topic, message_body, tag=""):
        """发送消息到指定主题"""
        print(f"[调试] 尝试发送消息到主题: {topic}")
        
        if not self.initialized or self.rocketmq_dll is None:
            print("[错误] 生产者尚未初始化")
            return False, "错误: 生产者尚未初始化"
        
        try:
            # 将Python字符串转换为C字符串
            print("[调试] 准备消息参数...")
            topic_c = ctypes.c_char_p(topic.encode('utf-8'))
            tag_c = ctypes.c_char_p(tag.encode('utf-8'))
            message_body_c = ctypes.c_char_p(message_body.encode('utf-8'))
            
            # 调用DLL中的发送消息函数
            print("[调试] 调用SendMessage函数...")
            result_ptr = self.rocketmq_dll.SendMessage(
                topic_c,
                tag_c,
                message_body_c
            )
            
            # 检查返回值是否为NULL
            if result_ptr is None:
                print("[警告] 调用DLL函数返回NULL")
                return False, "错误: 调用DLL函数返回NULL"
            
            # 将C字符串转换为Python字符串
            result = ctypes.string_at(result_ptr).decode('utf-8')
            print(f"[调试] 发送消息结果: {result}")
            
            # 处理发送结果
            if result.startswith("Success"):
                print("[调试] 消息发送成功")
                return True, result
            else:
                print(f"[错误] 消息发送失败: {result}")
                return False, result
        except Exception as e:
            print(f"[错误] 发送消息时发生错误: {e}")
            print(f"[错误详情] {traceback.format_exc()}")
            return False, str(e)
    
    def shutdown(self):
        """关闭生产者连接并释放资源"""
        print("[调试] 开始关闭RocketMQ生产者...")
        
        if not self.initialized or self.rocketmq_dll is None:
            print("[信息] 生产者未初始化，无需关闭")
            return True
        
        try:
            # 调用DLL中的关闭函数
            print("[调试] 调用ShutdownProducer函数...")
            result = self.rocketmq_dll.ShutdownProducer()
            
            if result == 0:
                print("[成功] 成功关闭RocketMQ生产者")
                self.initialized = False
                return True
            else:
                print(f"[错误] 关闭RocketMQ生产者失败 (代码: {result})")
                return False
        except Exception as e:
            print(f"[错误] 关闭过程中发生错误: {e}")
            print(f"[错误详情] {traceback.format_exc()}")
            return False


def main():
    print("===== RocketMQ Python生产者示例 ======")
    print(f"[系统信息] Python版本: {os.sys.version}")
    print(f"[系统信息] 操作系统: {os.name}")
    print(f"[系统信息] 当前进程ID: {os.getpid()}")
    
    # 创建生产者实例
    print("[调试] 创建RocketMQProducer实例...")
    producer = RocketMQProducer()
    
    try:
        # 初始化生产者
        print("[调试] 开始初始化生产者...")
        initialized = producer.initialize(
            name_server="192.168.1.2:9876",
            group="test_task_group"
        )
        
        if not initialized:
            print("[错误] 初始化失败，程序退出")
            return
        
        print("\n[状态] 开始发送测试消息...")
        print("====================================")
        
        # 发送一条测试消息
        print("[状态] 发送第一条测试消息...")
        success, result = producer.send_message(
            topic="wechat_task",
            tag="tag",
            message_body="这是一条来自Python生产者的测试消息！"
        )
        
        if success:
            print(f"[成功] 消息发送成功: {result}")
        else:
            print(f"[失败] 消息发送失败: {result}")
            
        # 发送多条不同内容的消息
        for i in range(2, 5):
            print(f"\n[状态] 发送第{i}条测试消息...")
            success, result = producer.send_message(
                topic="wechat_task",
                tag="tag",
                message_body=f"这是来自Python生产者的第{i}条测试消息，时间戳: {time.time()}"
            )
            
            if success:
                print(f"[成功] 消息发送成功: {result}")
            else:
                print(f"[失败] 消息发送失败: {result}")
            
            # 短暂休眠，避免消息发送过快
            time.sleep(1)
        
    except KeyboardInterrupt:
        print("\n====================================")
        print("[状态] 程序已被用户中断")
    except Exception as e:
        print(f"\n[严重错误] 程序发生未预期错误: {e}")
        print(f"[错误详情] {traceback.format_exc()}")
    finally:
        # 确保在程序退出前关闭生产者连接
        print("\n[清理] 正在清理资源...")
        producer.shutdown()
        print("[完成] 程序已安全退出")


if __name__ == "__main__":
    main()