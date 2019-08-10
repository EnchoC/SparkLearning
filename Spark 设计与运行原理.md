# Spark设计与运行原理

<a name="kZoKp"></a>
# 1. 简介
<a name="pDzHu"></a>
## 1.1 相对于Hadoop的优势
Hadoop最主要的缺陷是其**MapReduce计算模型延迟过高**，无法胜任实时、快速计算的需求，因而只适用于离线批处理的应用场景。<br />Hadoop还存在如下一些缺点：

- **表达能力**有限。计算都必须要转化成Map和Reduce两个操作，但这并不适合所有的情况，难以描述复杂的数据处理过程；
-   磁盘**IO开销**大。每次执行时都需要从磁盘读取数据，并且在计算完成后需要将中间结果写入到磁盘中，IO开销较大；
-  **延迟**高。一次计算可能需要分解成一系列按顺序执行的MapReduce任务，任务之间的衔接由于涉及到IO开销，会产生较高延迟。而且，在前一个任务执行完成之前，其他任务无法开始，难以胜任复杂、多阶段的计算任务。


<br />Spark在借鉴Hadoop MapReduce优点的同时，很好地解决了MapReduce所面临的问题。相比于MapReduce，Spark主要具有如下优点：

-   Spark的计算模式也属于MapReduce，但不局限于Map和Reduce操作，还提供了多种数据集操作类型，编程模型比MapReduce**更灵活**；
-  Spark提供了**内存计算**，中间结果直接放到内存中，带来了更高的迭代运算效率；最大特点
-  Spark**基于DAG的任务调度**执行机制，要优于MapReduce的迭代执行机制。

但Hadoop可以使用廉价的、异构的机器来做分布式存储与计算，但是，Spark对硬件的要求稍高一些，对内存与CPU有一定的要求。

<a name="ScLfa"></a>
## 1.2 Spark生态系统
大数据处理主要包括以下三个类型：

- 复杂的批量数据处理：时间跨度通常在数十分钟到数小时之间；
- 基于历史数据的交互式查询：时间跨度通常在数十秒到数分钟之间；
- 基于实时数据流的数据处理：时间跨度通常在数百毫秒到数秒之间。

Spark所提供的生态系统足以应对三种场景，即同时支持批处理、交互式查询和流数据处理。<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565338578848-8e43e2fb-4159-4ffb-beab-bb0f7edad1d3.png#align=left&display=inline&height=300&name=image.png&originHeight=285&originWidth=622&size=60745&status=done&width=654)<br />_BDAS架构_<br />_Spark的生态系统主要包含了Spark Core、Spark SQL、Spark Streaming、MLLib和GraphX 等组件，各个组件的具体功能如下：

- **Spark Core**：**包含Spark的基本功能**，如内存计算、任务调度、部署模式、故障恢复、存储管理等。Spark建立在统一的抽象RDD之上，使其可以以基本一致的方式应对不同的大数据处理场景；通常所说的Apache Spark，就是指Spark Core；
-  **Spark SQL**：允许开发人员直接**处理RDD**，同时也可查询Hive、HBase等外部数据源。Spark SQL的一个重要特点是其能够统一处理关系表和RDD，使得开发人员可以轻松地**使用SQL命令进行查询**，并进行更复杂的数据分析；
- **Spark Streaming**：支持高吞吐量、可容错处理的**实时流数据处理**，其核心思路是将流式计算分解成一系列短小的批处理作业。Spark Streaming支持多种数据输入源，如Kafka、Flume和TCP套接字等；
-  **MLlib（机器学习）**：提供了常用**机器学习算法的实现**，包括聚类、分类、回归、协同过滤等，降低了机器学习的门槛，开发人员只要具备一定的理论知识就能进行机器学习的工作；
- **GraphX（图计算）**：是Spark中用于**图计算的API**，可认为是Pregel在Spark上的重写及优化，Graphx性能良好，拥有丰富的功能和运算符，能在海量数据上自如地运行复杂的图算法。

<a name="oyBOp"></a>
# 2. 架构
<a name="WHBWP"></a>
## 2.1 基本概念
在具体讲解Spark运行架构之前，需要先了解几个重要的概念：

- **RDD**：是**弹性分布式数据集**（Resilient Distributed Dataset）的简称，是分布式内存的一个抽象概念，提供了一种高度受限的共享内存模型；
- **DAG**：是Directed Acyclic Graph（**有向无环图**）的简称，反映RDD之间的依赖关系；
- **Executor**：是运行在工作节点（Worker Node）上的**一个进程**，负责运行任务，并为应用程序存储数据；
- **应用**：用户编写的Spark应用程序；
- **任务**：运行在Executor上的工作单元；
- **作业**：一个作业包含多个RDD及作用于相应RDD上的各种操作；
- **阶段**：是作业的基本调度单位，一个作业会分为多组任务，每组任务被称为“阶段”，或者也被称为“任务集”。

<a name="KApX3"></a>
## 2.2 架构设计
![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565339402266-b70064eb-7f99-4c0d-9ef0-142ccd6a86e8.png#align=left&display=inline&height=322&name=image.png&originHeight=415&originWidth=761&size=76016&status=done&width=590)<br />_Spark运行架构_<br />Spark运行架构包括

- 集群资源管理器（Cluster Manager）可以是Spark自带的资源管理器，也可以是YARN或Mesos等资源管理框架。
- 运行作业任务的工作节点（Worker Node）、
- 每个应用的任务控制节点（Driver）
- 每个工作节点上负责具体任务的执行进程（Executor）

与Hadoop MapReduce计算框架相比，Spark所采用的Executor有两个优点：

1. 利用**多线程**来执行具体的任务，减少任务的启动开销，而Hadoop MapReduce采用的是进程模型；
1. Executor中有一个BlockManager存储模块，会将内存和磁盘共同作为存储设备，当需要多轮迭代计算时，可以将中间结果存储到这个存储模块里，下次需要时，就可以直接读该存储模块里的数据，而**不需要读写到HDFS等文件系统里，因而有效减少了IO开销**；或者在交互式查询场景下，预先将表缓存到该存储系统上，从而可以提高读写IO性能。

![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565339741017-35fa78e0-8da4-4717-913a-75df8adc9866.png#align=left&display=inline&height=341&name=image.png&originHeight=475&originWidth=908&size=107755&status=done&width=651)<br />_Spark中各种概念之间的相互关系_

- 一个应用Application由一个任务控制节点Driver和若干个作业Job构成
- 一个作业由多个阶段Stage构成
- 一个阶段由多个任务Task组成

当执行一个应用时，任务控制节点会向集群管理器Cluster Manager申请资源，启动Executor，并向Executor发送应用程序代码和文件，然后在Executor上执行任务，运行结束后，执行结果会返回给任务控制节点，或者写到HDFS或者其他数据库中。

<a name="jbXiI"></a>
## 2.3 Spark运行基本流程
![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565340112515-5d6a4690-50cd-4b8c-80b8-271e14a84b4e.png#align=left&display=inline&height=463&name=image.png&originHeight=625&originWidth=866&size=159595&status=done&width=641)<br />_运行基本流程图_

1. 当一个Spark应用被提交时，首先需要为这个应用构建起基本的运行环境，即由任务控制节点**Driver创建一个SparkContext**，由SparkContext负责和资源管理器Cluster Manager的通信以及进行资源的申请、任务的分配和监控等。SparkContext会向资源管理器注册并申请运行Executor的资源；
1. 资源管理器**为Executor分配资源**，并启动Executor进程，Executor运行情况将随着“心跳”发送到资源管理器上；
1. SparkContext**根据RDD的依赖关系构建DAG图**，DAG图提交给DAG调度器（DAGScheduler）进行解析，将**DAG图分解成多个“阶段”**（每个阶段都是一个任务集），并且计算出各个阶段之间的依赖关系，然后把一个个“任务集”提交给底层的任务调度器（TaskScheduler）进行处理；Executor向SparkContext申请任务，任务调度器将任务分发给Executor运行，同时，SparkContext将应用程序代码发放给Executor；
1. 任务在Executor上运行，把执行结果反馈给任务调度器，然后反馈给DAG调度器，运行完毕后写入数据并释放所有资源。

Spark运行架构具有以下特点

1. 每个应用都有自己专属的Executor进程，并且该进程在应用运行期间一直驻留。Executor进程**以多线程的方式**运行任务，**减少了多进程任务频繁的启动开销**，使得任务执行变得非常高效和可靠；
1. Spark**运行过程与资源管理器无关**，只要能够获取Executor进程并保持通信即可；
1. Executor上有一个BlockManager存储模块，类似于键值存储系统（把内存和磁盘共同作为存储设备），在处理迭代计算任务时，不需要把中间结果写入到HDFS等文件系统，而是直接放在这个存储系统上，后续有需要时就可以直接读取；在交互式查询场景下，也可以把表提前缓存到这个存储系统上，提高读写IO性能；
1. 任务采用了**数据本地性和推测执行等优化机制**。数据本地性是尽量将计算移到数据所在的节点上进行，即“计算向数据靠拢”，因为**移动计算比移动数据所占的网络资源要少得多**。而且，Spark采用了延时调度机制，可以在更大的程度上实现执行过程优化。比如，拥有数据的节点当前正被其他的任务占用，那么，在这种情况下是否需要将数据移动到其他的空闲节点呢？答案是不一定。因为，如果经过预测发现当前节点结束当前任务的时间要比移动数据的时间还要少，那么，调度就会等待，直到当前节点可用。

<a name="tY09g"></a>
# 3. RDD的设计与运行原理
> Spark的核心是建立在统一的抽象RDD之上，使得Spark的各个组件可以无缝进行集成，在同一个应用程序中完成大数据计算任务。

<a name="zqL7s"></a>
## 3.1 RDD设计背景
目前的**MapReduce框架都是把中间结果写入到HDFS中，带来了****大量的数据复制、磁盘IO和序列化开销**。虽然，类似Pregel等图计算框架也是将结果保存在内存当中，但是，这些框架只能支持一些特定的计算模式，并没有提供一种通用的数据抽象。RDD就是为了满足这种需求而出现的，它提供了一个抽象的数据架构，我们不必担心底层数据的分布式特性，只需将具体的应用逻辑表达为一系列转换处理，不同RDD之间的转换操作形成依赖关系，可以实现管道化，从而避免了中间结果的存储，大大降低了数据复制、磁盘IO和序列化开销。

<a name="tnf7G"></a>
## 3.2 RDD概念
一个RDD就是**一个分布式对象集合，本质上是一个只读的分区记录集合**，每个RDD可以分成多个分区，每个分区就是一个数据集片段，并且一个RDD的不同分区可以被保存到集群中不同的节点上，从而可以在集群中的不同节点上进行并行计算。RDD提供了一种高度受限的共享内存模型，即**RDD是只读的记录分区的集合，不能直接修改，只能基于稳定的物理存储中的数据集来创建RDD，或者通过在其他RDD上执行确定的转换操作（如map、join和groupBy）而创建得到新的RDD**。RDD提供了一组丰富的操作以支持常见的数据运算，分为“行动”（Action）和“转换”（Transformation）两种类型，前者用于执行计算并指定输出的形式，后者指定RDD之间的相互依赖关系。两类操作的主要区别是，转换操作（比如map、filter、groupBy、join等）接受RDD并返回RDD，而行动操作（比如count、collect等）接受RDD但是返回非RDD（即输出一个值或结果）。RDD提供的转换接口都非常简单，都是类似map、filter、groupBy、join等粗粒度的数据转换操作，而不是针对某个数据项的细粒度修改。因此，RDD比较适合对于数据集中元素执行相同操作的批处理式应用，而不适合用于需要异步、细粒度状态的应用，比如Web应用系统、增量式的网页爬虫等。正因为这样，这种粗粒度转换接口设计，会使人直觉上认为RDD的功能很受限、不够强大。但是，实际上RDD已经被实践证明可以很好地应用于许多并行计算应用中，可以具备很多现有计算框架（比如MapReduce、SQL、Pregel等）的表达能力，并且可以应用于这些框架处理不了的交互式数据挖掘应用。

Spark用**Scala语言实现了RDD的API**，可以通过调用API实现对RDD的各种操作。RDD典型的执行过程：

1.  RDD读入外部数据源（或者内存中的集合）进行创建；
1. RDD经过一系列的“转换”操作，每一次都会产生不同的RDD，供给下一个“转换”使用；
1. 最后一个RDD经“行动”操作进行处理，并输出到外部数据源（或者变成Scala集合或标量）。

RDD采用**惰性调用**，即在RDD的执行过程中，**真正的计算发生在RDD的“行动”操作**，对于“行动”之前的所有“转换”操作，Spark只是记录下“转换”操作应用的一些基础数据集以及RDD生成的轨迹，即相互之间的依赖关系，而**不会触发真正的计算**。

<a name="pHIRt"></a>
## 3.3 RDD特性
Spark采用RDD以后能够实现高效计算的主要原因如下：

1. **高效**的**容错性**。现有的分布式共享内存、键值存储、内存数据库等，为了实现容错，必须在集群节点之间进行数据复制或者记录日志，也就是在节点之间会发生大量的数据传输，这对于数据密集型应用而言会带来很大的开销。在RDD的设计中，数据只读，不可修改，如果需要修改数据，必须从父RDD转换到子RDD，由此在不同RDD之间建立了血缘关系。所以，RDD是一种天生具有容错机制的特殊集合，不需要通过数据冗余的方式（比如检查点）实现容错，而只需通过RDD父子依赖（血缘）关系重新计算得到丢失的分区来实现容错，无需回滚整个系统，这样就避免了数据复制的高开销，而且重算过程可以在不同节点之间并行进行，实现了高效的容错。此外，RDD提供的转换操作都是一些粗粒度的操作（比如map、filter和join），RDD依赖关系只需要记录这种粗粒度的转换操作，而不需要记录具体的数据和各种细粒度操作的日志（比如对哪个数据项进行了修改），这就大大降低了数据密集型应用中的容错开销；
1. **中间结果持久化到内存**。数据在内存中的多个RDD操作之间进行传递，不需要“落地”到磁盘上，避免了不必要的读写磁盘开销；
1. 存放的数据可以是Java对象，**避免了不必要的对象序列化和反序列化开销**。



<a name="afAKP"></a>
## 3.4 RDD之间的依赖关系
RDD中不同的操作会使得不同RDD中的分区会产生不同的依赖。RDD中的依赖关系分为窄依赖（Narrow Dependency）与宽依赖（Wide Dependency）。<br />总而言之，如果父RDD的一个分区只被一个子RDD的一个分区所使用就是窄依赖，否则就是宽依赖。窄依赖典型的操作包括map、filter、union等，宽依赖典型的操作包括groupByKey、sortByKey等。对于连接（join）操作，可以分为两种情况：<br />（1）对输入进行协同划分，属于窄依赖。所谓协同划分（co-partitioned）是指多个父RDD的某一分区的所有“键（key）”，落在子RDD的同一个分区内，不会产生同一个父RDD的某一分区，落在子RDD的两个分区的情况。<br />（2）对输入做非协同划分，属于宽依赖。对于窄依赖的RDD，可以以流水线的方式计算所有父分区，不会造成网络之间的数据混合。对于宽依赖的RDD，则通常伴随着Shuffle操作，即首先需要计算好所有父分区数据，然后在节点之间进行Shuffle。<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565342215172-6d45070e-c2a4-4e44-b6f3-2714fcf60510.png#align=left&display=inline&height=488&name=image.png&originHeight=657&originWidth=881&size=226008&status=done&width=654)<br />_窄依赖与宽依赖的区别_<br />Spark的这种依赖关系设计，使其具有了天生的容错性，大大加快了Spark的执行速度。因为，RDD数据集通过“血缘关系”记住了它是如何从其它RDD中演变过来的，血缘关系记录的是粗颗粒度的转换操作行为，当这个RDD的部分分区数据丢失时，它可以通过血缘关系获取足够的信息来重新运算和恢复丢失的数据分区，由此带来了性能的提升。<br />在两种依赖关系中，窄依赖的失败恢复更为高效，它只需要根据父RDD分区重新计算丢失的分区即可（不需要重新计算所有分区），而且可以并行地在不同节点进行重新计算。而对于宽依赖而言，单个节点失效通常意味着重新计算过程会涉及多个父RDD分区，开销较大。<br />此外，Spark还提供了数据检查点和记录日志，用于持久化中间RDD，从而使得在进行失败恢复时不需要追溯到最开始的阶段。在进行故障恢复时，Spark会对数据检查点开销和重新计算RDD分区的开销进行比较，从而自动选择最优的恢复策略。

<a name="19xEG"></a>
## 3.5 阶段的划分
Spark通过分析各个RDD的依赖关系生成了DAG，再通过分析各个RDD中的分区之间的依赖关系来决定如何划分阶段，具体划分方法是：
> 在DAG中进行反向解析，遇到宽依赖就断开，遇到窄依赖就把当前的RDD加入到当前的阶段中；将窄依赖尽量划分在同一个阶段中，可以实现流水线计算

例如，如图9-11所示，假设从HDFS中读入数据生成3个不同的RDD（即A、C和E），通过一系列转换操作后再将计算结果保存回HDFS。对DAG进行解析时，在依赖图中进行反向解析，由于从RDD A到RDD B的转换以及从RDD B和F到RDD G的转换，都属于宽依赖，因此，在宽依赖处断开后可以得到三个阶段，即阶段1、阶段2和阶段3。可以看出，在阶段2中，从map到union都是窄依赖，这两步操作可以形成一个流水线操作，比如，分区7通过map操作生成的分区9，可以不用等待分区8到分区9这个转换操作的计算结束，而是继续进行union操作，转换得到分区13，这样流水线执行大大提高了计算的效率。<br />    把一个DAG图划分成多个“阶段”以后，每个阶段都代表了一组关联的、相互之间没有Shuffle依赖关系的任务组成的任务集合。每个任务集合会被提交给任务调度器（TaskScheduler）进行处理，由任务调度器将任务分发给Executor运行。<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565342712606-3265bd3c-b5cc-4ee7-b506-597cfbda75b6.png#align=left&display=inline&height=409&name=image.png&originHeight=644&originWidth=1060&size=241804&status=done&width=673)<br />_根据RDD分区的依赖关系划分阶段_

<a name="IBU2w"></a>
## 3.6 RDD运行过程
RDD在Spark架构中的运行过程：<br />（1）创建RDD对象；<br />（2）SparkContext负责计算RDD之间的依赖关系，构建DAG；<br />（3）DAGScheduler负责把DAG图分解成多个阶段，每个阶段中包含了多个任务，每个任务会被任务调度器分发给各个工作节点（Worker Node）上的Executor去执行。<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565342918378-77b5606a-0a0e-406f-b2d4-3d4383394c24.png#align=left&display=inline&height=264&name=image.png&originHeight=387&originWidth=994&size=119173&status=done&width=678)<br />_RDD在Spark中的运行过程_<br />

<a name="Kg6rE"></a>
# 4. 部署模式
<a name="MaYah"></a>
## 4.1 三种部署方式
Spark应用程序在集群上部署运行时，可以由不同的组件为其提供资源管理调度服务（资源包括CPU、内存等）。比如，可以使用自带的独立集群管理器（standalone），或者使用YARN，也可以使用Mesos。三种不同类型的集群部署方式：

1. **standalone模式**

与MapReduce1.0框架类似，Spark框架本身也自带了完整的资源调度管理服务，可以独立部署到一个集群中，而不需要依赖其他系统来为其提供资源管理调度服务。在架构的设计上，Spark与MapReduce1.0完全一致，都是由一个Master和若干个Slave构成，并且以槽（slot）作为资源分配单位。不同的是，Spark中的**槽不再像MapReduce1.0那样分为Map槽和Reduce槽，而是只设计了统一的一种槽**提供给各种任务来使用。

2. **Spark on Mesos模式**

Mesos是一种**资源调度管理框架**，可以为运行在它上面的Spark提供服务。Spark on Mesos模式中，Spark程序所需要的各种资源，都由Mesos负责调度。由于Mesos和Spark存在一定的血缘关系，因此，Spark这个框架在进行设计开发的时候，就充分考虑到了对Mesos的充分支持，因此，相对而言，Spark运行在Mesos上，要比运行在YARN上更加灵活、自然。目前，Spark官方推荐采用这种模式，所以，许多公司在实际应用中也采用该模式。

3. **Spark on YARN模式**

Spark可运行于YARN之上，与Hadoop进行统一部署，即“Spark on YARN”，其架构如图9-13所示，资源管理和调度依赖YARN，分布式存储则依赖HDFS。<br />![image.png](https://cdn.nlark.com/yuque/0/2019/png/296079/1565343254395-9206d891-05e6-4dac-9d8a-f10ee59571d4.png#align=left&display=inline&height=312&name=image.png&originHeight=379&originWidth=852&size=48419&status=done&width=702)<br />_Spark on YARN架构_<br />_
<a name="CsPP5"></a>
## 4.2 从“Hadoop+Storm”架构转向Spark架构
为了能同时进行批处理与流处理，企业应用中通常会采用“Hadoop+Storm”的架构（也称为Lambda架构）。在下面这种部署架构中，Hadoop和Storm框架部署在资源管理框架YARN（或Mesos）之上，接受统一的资源管理和调度，并共享底层的数据存储（HDFS、HBase、Cassandra等）。Hadoop负责对批量历史数据的实时查询和离线分析，而Storm则负责对流数据的实时处理。<br />![](https://cdn.nlark.com/yuque/0/2019/jpeg/296079/1565343340188-8dbc19f6-abbf-40ac-bb9c-1e56507056e7.jpeg#align=left&display=inline&height=712&originHeight=712&originWidth=960&size=0&status=done&width=960)<br />_采用“Hadoop+Storm”部署方式的一个案例_<br />但是，上面这种架构部署较为繁琐。由于Spark同时支持批处理与流处理，因此，对于一些类型的企业应用而言，从“Hadoop+Storm”架构转向Spark架构（如图9-15所示）就成为一种很自然的选择。采用Spark架构具有如下优点：

- 实现一键式安装和配置、线程级别的任务监控和告警；
- 降低硬件集群、软件维护、任务监控和应用开发的难度；
- 便于做成统一的硬件、计算平台资源池。

需要说明的是，Spark Streaming的原理是将流数据分解成一系列短小的批处理作业，每个短小的批处理作业使用面向批处理的Spark Core进行处理，通过这种方式变相实现流计算，而不是真正实时的流计算，因而通常无法实现毫秒级的响应。因此，对于需要毫秒级实时响应的企业应用而言，仍然需要采用流计算框架（如Storm）。<br />![](https://cdn.nlark.com/yuque/0/2019/jpeg/296079/1565343340076-e606bbb5-37c3-4d43-859f-7a7296069701.jpeg#align=left&display=inline&height=631&originHeight=631&originWidth=495&size=0&status=done&width=495)<br />_用Spark架构同时满足批处理和流处理需求_
<a name="E2BRA"></a>
## 4.3 Hadoop和Spark的统一部署
一方面，由于Hadoop生态系统中的一些组件所实现的功能，目前还是无法由Spark取代的，比如，Storm可以实现毫秒级响应的流计算，但是，Spark则无法做到毫秒级响应。另一方面，企业中已经有许多现有的应用，都是基于现有的Hadoop组件开发的，完全转移到Spark上需要一定的成本。因此，在许多企业实际应用中，Hadoop和Spark的统一部署是一种比较现实合理的选择。<br />由于Hadoop MapReduce、HBase、Storm和Spark等，都可以运行在资源管理框架YARN之上，因此，可以在YARN之上进行统一部署。这些不同的计算框架统一运行在YARN中，可以带来如下好处：

-  计算资源按需伸缩；
-  不用负载应用混搭，集群利用率高；
-  共享底层存储，避免数据跨集群迁移。

![](https://cdn.nlark.com/yuque/0/2019/jpeg/296079/1565343340100-e513d7fc-f6cb-4849-ac04-28a45aa2ac01.jpeg#align=left&display=inline&height=342&originHeight=342&originWidth=852&size=0&status=done&width=852)<br />_Hadoop和Spark的统一部署_
