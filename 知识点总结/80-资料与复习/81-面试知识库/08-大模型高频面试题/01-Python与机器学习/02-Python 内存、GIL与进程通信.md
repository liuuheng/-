---
tags:
  - 大模型
  - 面试
source: "尚硅谷大模型技术之高频面试题-V2.1.6.docx"
source_blocks: "66-100"
reviewed: 2026-10-21
---

# Python 内存、GIL与进程通信

把对象生命周期、解释器并发和操作系统进程分开讨论。

前篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/01-Python与机器学习/01-Python 闭包、装饰器与异步调用|Python 闭包、装饰器与异步调用]] ｜ 后篇：[[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/01-Python与机器学习/03-数据结构、复杂度与常见算法|数据结构、复杂度与常见算法]]

## 深入理解（新增）

引用计数解释了很多对象何时释放，但循环引用仍需垃圾回收器处理。__del__ 的调用时机和退出阶段行为不能作为文件落盘、事务提交或释放锁的可靠协议；这些资源应交给 with、try/finally 或显式 close。

GIL 限制的是特定解释器构建中的 Python 执行，不等于所有线程都不能并行。I/O 等待和部分释放 GIL 的扩展可以并发；可选的 free-threaded 构建又有不同约束。选择线程或进程前，应同时检查解释器构建、扩展支持和任务是否主要耗在 Python 字节码上。

进程池增加了序列化和传输成本。大数组每次复制可能比计算更慢；共享内存能减少复制，却把生命周期与同步责任交回应用。不要把协程、线程、进程当成只按数量替换的三种加速开关。



## 核心代码（新增）

以下是可在 CPU 运行的机制或业务契约示例，完整源码与测试见 [[知识点总结/80-资料与复习/81-面试知识库/08-大模型高频面试题/06-代码实验/01-核心代码实验与验证范围|核心代码实验与验证范围]]。示例不包含完整模型训练或外部平台部署。

```python
import asyncio

async def bounded_map(items, operation, concurrency=4, timeout=1.0):
    semaphore = asyncio.Semaphore(concurrency)
    async def one(item):
        async with semaphore:
            return await asyncio.wait_for(operation(item), timeout=timeout)
    return await asyncio.gather(*(one(item) for item in items))

async def fake_operation(item):
    await asyncio.sleep(0.001)
    return item * 2

assert asyncio.run(bounded_map([1, 2], fake_operation)) == [2, 4]
```

## 追问与验证（新增）

分别用异步 sleep 和纯 Python 循环替换 operation，观察事件循环延迟；解释为何相同并发数不产生相同吞吐。

## 官方资料与论文

- [CPython free threading](https://docs.python.org/3/howto/free-threading-python.html)
- [asyncio 任务与超时](https://docs.python.org/3/library/asyncio-task.html)

核验日期：2026-10-21。链接中的 `stable`、`main` 或平台文档会继续变化；新增纯 Python/NumPy 实验与原文旧版框架代码已分开标注。

## 原文保留

> [!warning] 原文批注
> free-threaded 是构建及运行环境相关能力，不能从 Python 版本号直接断言当前进程没有 GIL；__del__ 也不能替代资源管理协议。

<!-- source-block:66 -->
### \*\*内存管理与垃圾回收

<!-- source-block:67 -->
#### Python 如何进行内存管理？

<!-- source-block:68 -->
Python 内存由私有堆空间管控，所有对象与数据结构置于此，程序员不能直接访问，内存管理器负责堆空间内存分配。其有内置垃圾收集器，借助对象引用计数等机制，回收未用内存释放给堆空间。

<!-- source-block:69 -->
#### 什么是垃圾回收？

<!-- source-block:70 -->
Python 为解决内存泄漏，采取对象引用计数，并基于此实现自动垃圾回收。当对象引用计数降为 0 ，内存会被回收。此外，其还处理循环引用等复杂状况。

<!-- source-block:71 -->
### 其他重要问题

<!-- source-block:72 -->
#### \*\*解释 Python 中的 GIL（全局解释器锁）？

<!-- source-block:73 -->
GIL 是 Python 解释器的一种机制，其保证任意时刻仅一个线程能执行 Python 字节码。它让 Python 多线程处理 I/O 密集型任务效率尚可，但执行 CPU 密集型任务时，多核心CPU只能发挥和单核CPU一样的性能。

<!-- source-block:74 -->
绕过GIL的方式：使用多个进程，每个进程有自己的Python解释器和GIL，然后多进程并行。除此之外还可以使用Cpython或者C扩展，可以释放GIL锁，以及Numpy和SciPy等库也可以不受GIL限制。

<!-- source-block:75 -->
注意：Python3.14的发布标志着GIL从强制变成了可选项。

<!-- source-block:76 -->
#### 如何在 Python 中进行文件删除操作？

<!-- source-block:77 -->
可借助 os 模块的 remove 或 unlink 函数，像 os.remove('filename.txt') 就能删除名为 filename.txt 的文件。

<!-- source-block:78 -->
#### 什么是Python中的迭代器？

<!-- source-block:79 -->
Python 里的迭代器就是能逐个取元素的对象，得实现\_\_iter\_\_和\_\_next\_\_两个方法，特点是懒加载 —— 用的时候才生成元素特省内存，而且只能往前取、取完就不能复用了。列表、字符串这些可迭代对象，调 iter () 就能转成迭代器，for 循环遍历其实就是自动调 next () 取元素，取完抛的异常会被自动捕获。迭代器主要就是用来处理大数据集，避免一次性加载占满内存的。

<!-- source-block:80 -->
#### \*\*什么是 Python 中的生成器？

<!-- source-block:81 -->
生成器是实现迭代器的简便方式，为含 yield 表达式的普通函数。其逐个生成值，可暂停和恢复执行，节省内存，适用于处理大序列数据场景。

<!-- source-block:82 -->
这里需要注意的是推导式的三个基本的\[\]列表,{}字典/集合，()生成器表达式，列表和字典推导式不涉及生成器/迭代器，因为这俩都是直接生成完整内容并加载到内存是完整容器，生成器是特殊的迭代器，不会一次性加载所有元素。

<!-- source-block:83 -->
#### \*\*多进程之间的通信？

<!-- source-block:84 -->
**管道（Pipe）**

<!-- source-block:85 -->
适用于两个进程之间的双向/单向通信（通常是父子进程或者是兄弟进程）。

<!-- source-block:86 -->
- 特点：基于内存实现，简单高效，无需额外中间介质，但不支持多个进程同时读写（容易发生冲突）

<!-- source-block:87 -->
**队列（Queue）**

<!-- source-block:88 -->
基于“管道 + 锁（多进程内置的互斥锁）”实现，支持多个进程之间的安全通信，是最常用的方法之一。

<!-- source-block:89 -->
- 特点：自带进程安全保护，适合实现“生产者-消费者”模型，使用简单，无需手动处理锁冲突。

<!-- source-block:90 -->
**共享内存（Value/Array）**

<!-- source-block:91 -->
让多个进程共享同一块内存区域，直接读写内存数据，速度最快。

<!-- source-block:92 -->
- 特点：高效适合大量数据传输，但无内置锁保护，需要手动加锁（如Lock）避免并发读写冲突。

<!-- source-block:93 -->
**管理器（Manager）**

<!-- source-block:94 -->
基于“服务进程 + 客户端代理”实现，自带进程安全锁，底层依托本地网络套接字通信。支持共享复杂数据结构（字典，列表等），可以跨多个进程甚至跨机使用。

<!-- source-block:95 -->
- 特点：功能强大，使用灵活，但基于网络通信，效率比共享内存低，适合需要共享复杂数据的场景。

<!-- source-block:96 -->
**补充：信号/事件（同步辅助）**

<!-- source-block:97 -->
不传输实际数据，主要用于进程间同步（通知对方如状态变化，如“数据已准备好”）

<!-- source-block:98 -->
- 特点：常配合其他通信方法使用，协调进程执行顺序，而非传递业务数据。

<!-- source-block:99 -->
#### 多个线程之间的通信？

<!-- source-block:100 -->
一般常用的有：直接共享全局变量/对象，实现简单但是没有内置线程安全保护，并发易发生错误，得手动加锁。队列方法和多进程队列类似，内置线程互斥锁，无需手动加锁，最常用。同步原语（Event/Condition）用于协调线程通信，不直接传递业务数据。线程之间通信的核心优势是共享进程内存，比进程通信简单高效。
