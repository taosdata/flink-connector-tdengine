# Flink 版本兼容方案(1.15 / 1.19 / 1.20)

本文描述 `flink-connector-tdengine` 当前如何用一个 jar 同时适配多个 Flink 版本(重点是 Flink 1.15 与 1.19),以及该方案尚未覆盖的风险与缺失。最后附 2026-10-07 的双集群实测验证结论。

---

## 1. 背景与目标

| 集群 | 版本 | 场景 |
|---|---|---|
| 华为云 DLI(Luxor) | Flink **1.15** | 生产部署,只接受 fat jar 提交 |
| 自建 standalone | Flink **1.19.3** | 验证/运行环境 |
| 编译基线 | Flink **1.20.3** | pom 中 `flink.version` |

目标:**产出单一 `*-dist.jar`,不做分支、不做多 profile**,同一制品可在 1.15 与 1.19(及 1.20)集群上运行。

## 2. 方案总览:三层防线

兼容性不是靠某一个开关,而是三层措施的叠加:

```
┌─────────────────────────────────────────────────────────────┐
│ ① 编译期:只用 1.15 就存在的公共 API 子集                     │
│    (以 1.20.3 编译,但刻意避开 1.15 之后引入/改动的签名)      │
├─────────────────────────────────────────────────────────────┤
│ ② 装配期:fat jar 剥离 Flink 自身与 janino,合并 SPI          │
│    (assembly-jar.xml 的 excludes + metaInf-services)        │
├─────────────────────────────────────────────────────────────┤
│ ③ 运行期:零反射、零版本探测                                  │
│    (不写 Class.forName(flink类)/getMethod,行为完全静态)      │
└─────────────────────────────────────────────────────────────┘
```

为什么这样组合:

- **只做"编译时降 API"不剥离类** → fat jar 里的 1.20 类在 1.15 集群上与运行时类混载,`ServiceLoader` 实例化外来工厂抛 `IllegalAccessError` / `ServiceConfigurationError`(实测,见 §5)。
- **只剥离类不降 API** → 编译能过,但 1.15 运行时缺 1.19+ 才有的构造器,直接 `NoSuchMethodError`。
- **剥离 Flink 不剥离 janino** → janino(`org.codehaus.*`)是 flink-table-planner 的传递依赖,groupId 不是 `org.apache.flink`,排除规则匹配不到;fat jar 里残留的 janino 会在 Calcite 首次编译 metadata handler 时双重加载,抛 `CompilerFactory cannot be cast to ICompilerFactory`。

### 2.1 第一层:API 子集选择(源码层)

连接器重度使用的 Flink 接口及其跨版本处理方式:

| Flink API | 用途 | 跨版本处理 |
|---|---|---|
| `Sink`(sink2)/`SinkWriter` | Sink 实现 | 1.15 存在 `createWriter(InitContext)`;1.19 起 deprecated 但有 default 方法桥接。**只实现旧签名**,两个版本都能跑 |
| `SingleThreadMultiplexSourceReaderBase` 5 参构造器 | Source/CDC Reader | 用 `(elementsQueue, fetcherManager, recordEmitter, config, readerContext)`。**不用** 1.19 引入的 4 参重载 |
| `SingleThreadFetcherManager(queue, supplier)` 2 参构造器 | Fetcher 管理 | **不用** 1.19 引入的 `(Supplier, Configuration)` 重载 |
| `Source`/`SourceReader`/`SplitEnumerator`/`SourceSplit`(FLIP-27) | Source 体系 | 1.15 与 1.19 签名一致,直接实现 |
| `DynamicTableSourceFactory`/`DynamicTableSinkFactory`/`FactoryUtil` | Table 工厂 | 1.14+ 稳定 |
| `DataStreamScanProvider` + `ProviderContext` | Table→DataStream | 1.15 引入,1.19 一致 |
| `SinkV2Provider` | Table Sink 接入 | 1.15 引入,1.19 一致 |
| `CatalogTable.getResolvedSchema()` | 表 schema | 1.14+ 稳定 |

代码中对应的刻意选择以 `@SuppressWarnings("deprecation")` 标注并附 javadoc 说明,位置:

- `src/main/java/com/taosdata/flink/sink/TDengineSink.java`
- `src/main/java/com/taosdata/flink/source/reader/TDengineSourceReader.java`
- `src/main/java/com/taosdata/flink/cdc/reader/TDengineCdcReader.java`
- `src/main/java/com/taosdata/flink/cdc/reader/TDengineCdcFetcherManager.java`

注意:**编写作业侧代码同样受此约束**。例如 `env.enableCheckpointing(long)` 不带 `CheckpointingMode` 枚举的版本可用(该枚举 1.15 在 `org.apache.flink.streaming.api`,1.20 移到 `org.apache.flink.core.execution`,包位置不同)。

### 2.2 第二层:装配(`src/main/assembly/assembly-jar.xml`)

```xml
<containerDescriptorHandlers>
  <!-- 合并(而非覆盖)各依赖 jar 的 SPI 文件:
       否则连接器的 Factory 注册会被依赖的同名文件覆盖 -->
  <handlerName>metaInf-services</handlerName>
</containerDescriptorHandlers>
...
<excludes>
  <!-- Flink 类不入 fat jar(由集群 lib 提供);janino 同理 -->
  <exclude>org.apache.flink:*</exclude>
  <exclude>org.codehaus.janino:*</exclude>
</excludes>
```

两个细节:

1. **`metaInf-services` 是合并语义**。默认行为是后一个依赖的 SPI 文件整体覆盖前一个——曾导致 fat jar 里 `META-INF/services/org.apache.flink.table.factories.Factory` 丢失 `TDengineDynamicTableFactory`,用户侧报 `Cannot discover a connector using option: 'connector'='tdengine-connector'`。
2. **剥离必须在 assembly 的 runtime dependencySet 上做**,而不是只在 pom 里改 scope。剥离后 SPI 合并文件中的外来 Factory 条目对应的类已不在 jar 里,ServiceLoader 遍历到死条目会报 `ServiceConfigurationError`,因此剥离后 SPI 文件实际只剩 `com.taosdata.flink.table.TDengineDynamicTableFactory` 一行,恰好正确。

### 2.3 第三层:无反射

全仓库对 Flink 类**没有任何** `Class.forName`/`getMethod` 调用(唯一的 `Class.forName` 在 `TDengineSink.java:77`,目标是 TDengine JDBC 驱动类,用于探测 WS 能力)。版本分支逻辑只有一处:`VersionComparator` 比较 **TDengine 服务器版本**(决定 SQL writer vs STMT2 writer),与 Flink 版本无关。

这意味着 jar 的行为完全由静态链接决定,不存在"运行时探测失败走错分支"这类隐患;代价是放弃了在 1.19+ 上自动切换到新 API 的机会(见 §6)。

## 3. 交付物形态

| 产物 | 内容 | 用途 |
|---|---|---|
| `flink-connector-tdengine-<ver>.jar` | 仅连接器类 | 用户提供 Flink 依赖时(classloader 隔离的 per-job 集群) |
| `flink-connector-tdengine-<ver>-dist.jar` | 连接器 + 第三方依赖,**不含** Flink/janino 类 | DLI 等 fat jar 提交场景(**唯一实测通过的形态**) |
| `*-jar-with-dependencies.jar` | 未剥离的 fat jar | 兼容遗留,含 Flink 类,**不要**提交到低版本集群 |

## 4. 已验证矩阵(2026-10-07)

构建:HEAD `0cd82e0`(commit "adapt to flink 1.15"),JDK 21 编译,`flink.version=1.20.3`。
环境:172.16.22.3 双 standalone 集群(8081=Flink 1.15.4,8082=Flink 1.19.3),TDengine 3.4.1.5 同机。
方法:dist jar 注入验证作业类(仅用 1.15/1.19 公共 API),REST `/jars/upload` + `/jars/:id/run` 提交,sink 侧 checksum + 活性探针行核对。

| 验证项 | 1.15.4 | 1.19.3 |
|---|---|---|
| Table batch(source→sink) | ✅ FINISHED,31 行 checksum 逐字段一致 | ✅ 5s FINISHED,32 行 checksum 一致 |
| CDC(topic 消费,earliest) | ✅ 存量全量消费 | ✅ 28 个 checkpoint 全成 |
| CDC 活性探针(插入特征行) | ✅ 15s 内流入 sink,数值正确 | ✅ 同 |
| savepoint stop→恢复 | ✅ `restored=4`,停机间隙写入也追上 | ✅ `restored=2`,恢复后探针正常 |
| 冷 JVM 首次加载(类混载/janino CCE) | ✅ 无 | ✅ 无(该集群首次跑此 jar) |

## 5. 已知失效模式(历史踩坑记录)

以下是本方案演进过程中**实际发生过**的失败,每一条都对应 §2 中某一层防线的引入:

1. fat jar 覆盖 SPI → `Cannot discover a connector`(修复:`metaInf-services`)。
2. fat jar 捆绑 1.20 Flink 类 → 1.15 集群 `IllegalAccessError`(修复:assembly excludes)。
3. fat jar 残留 janino → Calcite 首编译 `ClassCastException: CompilerFactory`(修复:excludes 加 `org.codehaus.janino:*`)。**该错误只在冷 JVM 首次触发出必现,常驻 JM 会缓存编译结果而掩盖问题**——本地复验必须重启 JM。
4. REST 提交的作业代码调 `TableResult.await()` → `WebSubmissionJobClient` 必炸(作业本身已提交成功)。这是作业侧写法限制,不是连接器问题,但容易误判。

## 6. 缺失与风险(方案尚未覆盖的部分)

### 6.1 结构性缺失

- **无编译期/CI 防回归机制**。整个方案依赖"源码恰好只用 1.15 API 子集"这一人工约定——没有任何工具保证它。误调一个 1.19+ 的构造器,以 1.20.3 编译照样通过,直到 1.15 集群运行时才抛 `NoSuchMethodError`。CI(build.yml)只在单一 `flink.version` 下跑测试,测的是 1.20 行为,不是 1.15 行为。
- **README 声称 "Flink 1.19.0 or above" 与实际能力不符**(实测 1.15 可用),且未在任何面向用户的地方说明多版本适配形态与限制。
- **dist jar 体积与内容失控**:~95MB 中泄漏约 1.1 万个 testcontainers 类、netty、scala 编译器、ibm 消息客户端等(test 依赖被打入 runtime)。无依赖瘦身机制,后续任何依赖升级都可能把更大的东西带进去。
- **pom 中 Flink 依赖全部 `compile` scope 而非 `provided`**:对 per-job 场景是必要的,但意味着每次 `mvn package` 产物里都可能携带与目标集群冲突的类,全靠 assembly 一层兜底。

### 6.2 向前兼容风险(升级 Flink 时)

**硬天花板:Flink 1.20。** 以下事实已于 2026-10-08 逐一验证(javap 对比 1.20.3 / 2.2.1 制品 + Flink master 源码):

- **当前零功能损失**:`Sink$InitContext` 与 `WriterInitContext` 在 1.20.3 中方法面**完全一致**(8 个方法逐一对比,含 default `metadataConsumer()`);旧构造器在新 runtime 行为等价。在 1.15–1.20 目标范围内,不切换新 API 是免费的。
- **Flink 2.0 已删除旧 API**:master 的 `Sink` 接口只剩 `createWriter(WriterInitContext)`;flink-connector-base 2.2.1 中,5 参 `SourceReaderBase(elementsQueue, ...)` 构造器与 2 参 `SingleThreadFetcherManager(elementsQueue, supplier)` 构造器均已移除。
- **选择的悖论**:为兼容 1.15 而避开的 1.19 新重载(`(Supplier, Configuration)` FetcherManager、4 参 Reader),恰是 2.x 中仅存的幸存者。"为 1.15 选择"与"面向未来选择"在这 4 个调用点上完全相反——单 jar 策略自 Flink 2.0 起无论如何失效,必须分叉构建。
- **新能力门票被锁**:新能力均挂在新构造器上——`RecordEvaluator`(批量 limit 下推/提前终止)、`RateLimiterStrategy`(2.x 源端限流)。当前未用到所以无痛,但新增这类特性即撞 1.15 兼容墙。
- **维护性债**:4 处 `@SuppressWarnings("deprecation")` 掩盖了"走删除倒计时路径"的信号;上游对 deprecated 路径的修复投入会衰减。迁移到 2.x 时需一次性改写 4 个文件(`TDengineSink`/`TDengineSourceReader`/`TDengineCdcReader`/`TDengineCdcFetcherManager`),工作量约半天,但无法提前渐进完成。
- `BinaryRowData`/`BinaryRowWriter` 属 `table.data.binary` 内部包,二进制布局无契约保证;serializer 依赖其二进制格式,大版本升级需回归全类型测试。
- savepoint **跨 Flink 版本恢复**(如 1.15 的 savepoint 在 1.19 恢复)不在本方案能力范围内,也未经测试;连接器自有状态(split/enum/offset)的序列化格式变更同样会使旧 savepoint 失效。

### 6.3 未验证范围

- 1.16 / 1.17 / 1.18 未实测(理论上介于 1.15 与 1.19 之间,风险较低,但无证据)。
- 1.20 集群未实测(编译基线即 1.20,风险最低,但同样无运行证据)。
- 精确一次(exactly-once)语义、checkpoint 跨版本恢复、SQL JOIN/维表等复杂拓扑未在 1.19 上验证(1.19 侧只做了 batch/CDC/savepoint 主链路)。
- DLI(Luxor)上的 1.15 与自建 1.15.4 行为是否完全一致,仅以 2026-09 生产 trace 佐证,未做对照实验。

## 7. 建议的补强方向

按优先级:

1. **CI 双版本矩阵**:build.yml 增加 flink.version=1.15.4 的编译+测试 job(仅需覆盖编译与单测,不必双集群),防止 API 子集被无意破坏;有条件时加 1.15/1.19 REST 冒烟(参考本文 §4 方法)。
2. **更新 README/官方文档**:明确"单 jar 支持的 Flink 版本范围"与三种 jar 形态的适用场景(见 §3)。
3. **依赖瘦身**:test 依赖 scope 收紧(test/provided),或 assembly 增加显式 includes,把 dist jar 控制在连接器必需依赖内。
4. 若未来需支持 Flink 2.x(FLIP-成熟版 Sink API 落地后),再评估"版本分支构建"或"反射适配层",当前不建议提前引入。

---

*最后更新:2026-10-07,基于 commit `0cd82e0` 的代码与当日双集群验证结果。*
