package main

/*
#include <stdlib.h>
*/
import (
	"C"
	"context"
	"flag"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
	"sync"
	"time"
)

// 全局变量，用于存储消费者实例和消息通道
type ConsumerState struct {
	consumer    rocketmq.PushConsumer
	messageChan chan string
	config      *MQConfig
	mutex       sync.Mutex
	isRunning   bool
	lastError   string // 存储最后一个错误信息
}

var globalConsumer *ConsumerState

// 全局变量，用于存储生产者实例
type ProducerState struct {
	producer  rocketmq.Producer
	config    *MQConfig
	mutex     sync.Mutex
	isRunning bool
	lastError string // 存储最后一个错误信息
}

var globalProducer *ProducerState

// MQConfig 结构体用于存储RocketMQ配置
type MQConfig struct {
	NSResolver string
	Topic      string
	Group      string
	Tag        string
}

var (
	nsResolver = flag.String("ns", "192.168.1.2:9876", "RocketMQ NameServer address")
	topic      = flag.String("topic", "task_topic", "MQ Topic to consume")
	group      = flag.String("group", "task_group", "Consumer group")
	tag        = flag.String("tag", "tag", "Message tag filter")

	// 初始化全局消费者状态
	consumerOnce sync.Once
	producerOnce sync.Once
)

// GetMQConfig 从命令行参数和环境变量获取MQ配置
func GetMQConfig() *MQConfig {
	flag.Parse()

	// 从命令行参数获取配置
	config := &MQConfig{
		NSResolver: *nsResolver,
		Topic:      *topic,
		Group:      *group,
		Tag:        *tag,
	}

	// 从环境变量覆盖配置
	if ns := os.Getenv("ROCKETMQ_NS"); ns != "" {
		config.NSResolver = ns
	}
	if t := os.Getenv("ROCKETMQ_TOPIC"); t != "" {
		config.Topic = t
	}
	if g := os.Getenv("ROCKETMQ_GROUP"); g != "" {
		config.Group = g
	}
	if tag := os.Getenv("ROCKETMQ_TAG"); tag != "" {
		config.Tag = tag
	}

	return config
}

// 初始化全局消费者状态（单例模式）
func initGlobalConsumer() {
	consumerOnce.Do(func() {
		globalConsumer = &ConsumerState{
			messageChan: make(chan string, 100), // 更大的缓冲容量
			isRunning:   false,
		}
	})
}

// 初始化全局生产者状态（单例模式）
func initGlobalProducer() {
	producerOnce.Do(func() {
		globalProducer = &ProducerState{
			isRunning: false,
		}
	})
}

//export InitializeProducer
func InitializeProducer(
	nsAddr *C.char,
	group *C.char,
) C.int {
	fmt.Println("[Go-DLL] InitializeProducer called")

	// 初始化全局生产者（单例模式）
	initGlobalProducer()

	globalProducer.mutex.Lock()
	defer globalProducer.mutex.Unlock()

	// 检查生产者是否已经在运行
	if globalProducer.isRunning {
		fmt.Println("[Go-DLL] Producer is already running")
		return 1 // 表示已在运行
	}

	// 重置错误信息
	globalProducer.lastError = ""

	// 检查传入的参数是否有效
	if nsAddr == nil || group == nil {
		fmt.Println("[Go-DLL] Error: One or more parameters are NULL")
		globalProducer.lastError = "One or more parameters are NULL"
		return -4 // 参数无效
	}

	// 转换C字符串为Go字符串
	ns := C.GoString(nsAddr)
	groupStr := C.GoString(group)

	// 验证参数内容
	if ns == "" || groupStr == "" {
		fmt.Printf("[Go-DLL] Error: Invalid parameters - NS: '%s', Group: '%s'\n",
			ns, groupStr)
		globalProducer.lastError = "Invalid parameters: NameServer or Group cannot be empty"
		return -4 // 参数无效
	}

	// 保存配置
	globalProducer.config = &MQConfig{
		NSResolver: ns,
		Group:      groupStr,
	}

	fmt.Printf("[Go-DLL] Initializing producer with: NS=%s, Group=%s\n",
		ns, groupStr)

	// 配置RocketMQ生产者
	fmt.Println("[Go-DLL] Creating producer...")
	p, err := rocketmq.NewProducer(
		producer.WithGroupName(groupStr),
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{ns})),
		producer.WithRetry(2), // 设置重试次数
	)
	if err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to create producer: %v\n", err)
		globalProducer.lastError = fmt.Sprintf("Create producer failed: %v", err)
		return -1 // 创建失败
	}
	fmt.Println("[Go-DLL] Producer created successfully")

	// 启动生产者
	fmt.Println("[Go-DLL] Starting producer...")
	if err := p.Start(); err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to start producer: %v\n", err)
		globalProducer.lastError = fmt.Sprintf("Start producer failed: %v", err)
		return -2 // 启动失败
	}

	// 保存生产者实例并标记为运行中
	globalProducer.producer = p
	globalProducer.isRunning = true
	fmt.Println("[Go-DLL] Producer started successfully")

	return 0 // 成功
}

//export InitializeConsumer
func InitializeConsumer(
	nsAddr *C.char,
	topic *C.char,
	group *C.char,
	tag *C.char,
) C.int {
	fmt.Println("[Go-DLL] InitializeConsumer called")

	// 初始化全局消费者（单例模式）
	initGlobalConsumer()

	globalConsumer.mutex.Lock()
	defer globalConsumer.mutex.Unlock()

	// 检查消费者是否已经在运行
	if globalConsumer.isRunning {
		fmt.Println("[Go-DLL] Consumer is already running")
		return 1 // 表示已在运行
	}

	// 重置错误信息
	globalConsumer.lastError = ""

	// 检查传入的参数是否有效
	if nsAddr == nil || topic == nil || group == nil || tag == nil {
		fmt.Println("[Go-DLL] Error: One or more parameters are NULL")
		globalConsumer.lastError = "One or more parameters are NULL"
		return -4 // 参数无效
	}

	// 转换C字符串为Go字符串
	ns := C.GoString(nsAddr)
	topicStr := C.GoString(topic)
	groupStr := C.GoString(group)
	tagStr := C.GoString(tag)

	// 验证参数内容
	if ns == "" || topicStr == "" || groupStr == "" {
		fmt.Printf("[Go-DLL] Error: Invalid parameters - NS: '%s', Topic: '%s', Group: '%s'\n",
			ns, topicStr, groupStr)
		globalConsumer.lastError = "Invalid parameters: NameServer, Topic or Group cannot be empty"
		return -4 // 参数无效
	}

	// 保存配置
	globalConsumer.config = &MQConfig{
		NSResolver: ns,
		Topic:      topicStr,
		Group:      groupStr,
		Tag:        tagStr,
	}

	fmt.Printf("[Go-DLL] Initializing consumer with: NS=%s, Topic=%s, Group=%s, Tag=%s\n",
		ns, topicStr, groupStr, tagStr)

	// 配置RocketMQ消费者
	fmt.Println("[Go-DLL] Creating push consumer...")
	pushConsumer, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(groupStr),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{ns})),
		consumer.WithConsumerModel(consumer.Clustering),                // 使用集群消费模式
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset), // 从最早的消息开始消费
		consumer.WithPullInterval(time.Millisecond*100),                // 设置拉取间隔
	)
	if err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to create consumer: %v\n", err)
		globalConsumer.lastError = fmt.Sprintf("Create consumer failed: %v", err)
		return -1 // 创建失败
	}
	fmt.Println("[Go-DLL] Push consumer created successfully")

	// 订阅主题
	fmt.Printf("[Go-DLL] Subscribing to topic: %s with tag: %s\n", topicStr, tagStr)
	err = pushConsumer.Subscribe(
		topicStr,
		consumer.MessageSelector{Type: consumer.TAG, Expression: tagStr},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, msg := range msgs {
				// 记录收到消息
				// fmt.Printf("[Go-DLL] Received message: %s\n", string(msg.Body))
				// 将消息放入通道，不阻塞
				select {
				case globalConsumer.messageChan <- string(msg.Body):
					// 成功放入通道
				default:
					fmt.Println("[Go-DLL] Warning: Message channel is full, dropping message")
				}
			}
			return consumer.ConsumeSuccess, nil
		},
	)
	if err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to subscribe: %v\n", err)
		globalConsumer.lastError = fmt.Sprintf("Subscribe failed: %v", err)
		return -2 // 订阅失败
	}
	fmt.Println("[Go-DLL] Subscription successful")

	// 启动消费者
	fmt.Println("[Go-DLL] Starting consumer...")
	if err := pushConsumer.Start(); err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to start consumer: %v\n", err)
		globalConsumer.lastError = fmt.Sprintf("Start consumer failed: %v", err)
		return -3 // 启动失败
	}

	// 保存消费者实例并标记为运行中
	globalConsumer.consumer = pushConsumer
	globalConsumer.isRunning = true
	fmt.Println("[Go-DLL] Consumer started successfully")

	return 0 // 成功
}

//export GetNextMessage
func GetNextMessage(timeoutMs C.int) *C.char {
	fmt.Printf("[Go-DLL] GetNextMessage called with timeout: %d ms\n", timeoutMs)

	// 初始化全局消费者（单例模式）
	initGlobalConsumer()

	globalConsumer.mutex.Lock()
	running := globalConsumer.isRunning
	globalConsumer.mutex.Unlock()

	// 检查消费者是否在运行
	if !running {
		fmt.Println("[Go-DLL] Error: Consumer is not initialized or not running")
		// 静态错误信息，不需要Python释放
		return C.CString("Error: Consumer not initialized")
	}

	// 计算超时时间
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second // 默认超时10秒
		fmt.Printf("[Go-DLL] Using default timeout: %v\n", timeout)
	} else {
		fmt.Printf("[Go-DLL] Using specified timeout: %v\n", timeout)
	}

	// 创建定时器
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	fmt.Println("[Go-DLL] Waiting for message...")

	// 等待消息或超时
	select {
	case message := <-globalConsumer.messageChan:
		// 成功接收到消息
		fmt.Printf("[Go-DLL] Message received, length: %d bytes\n", len(message))
		// 处理空消息情况
		if len(message) == 0 {
			fmt.Println("[Go-DLL] Warning: Received empty message")
			// 静态字符串，不需要释放
			return C.CString("EMPTY_MESSAGE")
		}
		// 返回需要被释放的动态字符串
		return C.CString(message)
	case <-timer.C:
		// 超时
		fmt.Println("[Go-DLL] Timeout waiting for message")
		// 静态字符串，不需要释放
		return C.CString("TIMEOUT")
	}
}

//export ShutdownProducer
func ShutdownProducer() C.int {
	fmt.Println("[Go-DLL] ShutdownProducer called")

	// 初始化全局生产者（单例模式）
	initGlobalProducer()

	globalProducer.mutex.Lock()
	defer globalProducer.mutex.Unlock()

	// 检查生产者是否在运行
	if !globalProducer.isRunning {
		fmt.Println("[Go-DLL] Producer is not running")
		return 1 // 表示未运行
	}

	// 检查生产者实例是否有效
	if globalProducer.producer == nil {
		fmt.Println("[Go-DLL] Error: Producer instance is nil but marked as running")
		globalProducer.isRunning = false // 重置状态
		return -1                        // 关闭失败
	}

	// 关闭生产者
	fmt.Println("[Go-DLL] Shutting down producer...")
	if err := globalProducer.producer.Shutdown(); err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to shutdown producer: %v\n", err)
		return -1 // 关闭失败
	}

	// 重置状态
	globalProducer.producer = nil
	globalProducer.isRunning = false

	fmt.Println("[Go-DLL] Producer shutdown successfully")
	return 0 // 成功
}

//export ShutdownConsumer
func ShutdownConsumer() C.int {
	fmt.Println("[Go-DLL] ShutdownConsumer called")

	// 初始化全局消费者（单例模式）
	initGlobalConsumer()

	globalConsumer.mutex.Lock()
	defer globalConsumer.mutex.Unlock()

	// 检查消费者是否在运行
	if !globalConsumer.isRunning {
		fmt.Println("[Go-DLL] Consumer is not running")
		return 1 // 表示未运行
	}

	// 检查消费者实例是否有效
	if globalConsumer.consumer == nil {
		fmt.Println("[Go-DLL] Error: Consumer instance is nil but marked as running")
		globalConsumer.isRunning = false // 重置状态
		return -1                        // 关闭失败
	}

	// 关闭消费者
	fmt.Println("[Go-DLL] Shutting down consumer...")
	if err := globalConsumer.consumer.Shutdown(); err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to shutdown consumer: %v\n", err)
		return -1 // 关闭失败
	}

	// 重置状态
	globalConsumer.consumer = nil
	globalConsumer.isRunning = false

	// 清空消息通道
	fmt.Println("[Go-DLL] Clearing message channel...")
	clear(globalConsumer.messageChan)

	fmt.Println("[Go-DLL] Consumer shutdown successfully")
	return 0 // 成功
}

//export SendMessage
func SendMessage(
	topic *C.char,
	tag *C.char,
	messageBody *C.char,
) *C.char {
	fmt.Println("[Go-DLL] SendMessage called")

	// 初始化全局生产者（单例模式）
	initGlobalProducer()

	globalProducer.mutex.Lock()
	defer globalProducer.mutex.Unlock()

	// 检查生产者是否在运行
	if !globalProducer.isRunning || globalProducer.producer == nil {
		fmt.Println("[Go-DLL] Error: Producer is not initialized or not running")
		return C.CString("Error: Producer not initialized")
	}

	// 检查参数是否有效
	if topic == nil || messageBody == nil {
		fmt.Println("[Go-DLL] Error: Topic or message body cannot be NULL")
		return C.CString("Error: Topic or message body cannot be NULL")
	}

	// 转换C字符串为Go字符串
	topicStr := C.GoString(topic)
	tagStr := C.GoString(tag)
	msgBody := C.GoString(messageBody)

	// 验证参数内容
	if topicStr == "" || msgBody == "" {
		fmt.Printf("[Go-DLL] Error: Invalid parameters - Topic: '%s', MessageBody: '%s'\n",
			topicStr, msgBody)
		return C.CString("Error: Topic or message body cannot be empty")
	}

	// 创建消息
	message := primitive.NewMessage(
		topicStr,
		[]byte(msgBody),
	)

	// 设置标签
	if tagStr != "" {
		message.WithTag(tagStr)
	}

	fmt.Printf("[Go-DLL] Sending message to topic: %s, tag: %s, body length: %d bytes\n",
		topicStr, tagStr, len(msgBody))

	// 发送消息
	result, err := globalProducer.producer.SendSync(context.Background(), message)
	if err != nil {
		fmt.Printf("[Go-DLL] Error: Failed to send message: %v\n", err)
		return C.CString(fmt.Sprintf("Error: %v", err))
	}

	// 检查发送结果
	if result.Status != primitive.SendOK {
		fmt.Printf("[Go-DLL] Error: Send message failed with status: %v\n", result.Status)
		return C.CString(fmt.Sprintf("Error: Send failed with status %v", result.Status))
	}

	// 发送成功
	fmt.Printf("[Go-DLL] Message sent successfully, MsgID: %s\n", result.MsgID)
	return C.CString(fmt.Sprintf("Success: MsgID=%s", result.MsgID))
}

// 为了避免内存管理问题，函数占位符
//export FreeString
func FreeString(str *C.char) {
	fmt.Println("[Go-DLL] FreeString called - This function is now a placeholder")
	fmt.Println("[Go-DLL] Memory management is now handled internally by Go")
}

// 清除通道中的所有消息
func clear(ch chan string) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func main() {
	// 1. 初始化消费者
	InitializeConsumer(
		C.CString("192.168.1.2:9876"),
		C.CString("tset_task"),
		C.CString("test_task_group"),
		C.CString("idxxxx"),
	)

	// 2. 循环获取消息
	for i := 0; i < 30; i++ {
		msg := GetNextMessage(5000)
		fmt.Printf("Received message: %s\n", C.GoString(msg))
	}
	//
	// // 3. 关闭消费者
	ShutdownConsumer()
}
