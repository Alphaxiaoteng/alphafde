# 拆解中国企业大模型落地的"最后一公里"：如何用非标 DAG 与本地私有网关穿透传统工业软件

**发布团队：AlphaFDE 现场工程交付团队（中国 · 杭州 · 深圳）**
**发布日期：2025年7月**

---

## 前言：那些沉默死掉的 Demo

过去两年，我的团队在华东、华南的制造与供应链企业现场做了不下四十个大模型落地项目的前期评估。其中超过六成的甲方，在我们到访之前，已经和某家大厂或创业公司跑过"AI 助手"概念验证。他们的反馈几乎一字不差：

"演示的时候挺好用，接上我们自己的系统就废了。"

这不是偶然现象，而是当下企业 AI 市场的系统性失真。市面上 90% 的企业大模型演示建立在一套高度简化的假设之上：数据全在云端、接口格式标准、网络延迟极低、业务逻辑线型推进。然而在真实的中国工业现场，工程师面临的是另一套物理规律：

- 核心 ERP 是 2008 年部署的金蝶 K3 或用友 U8，没有任何开放 API；
- 生产设备通过 Modbus TCP 或 OPC-UA 暴露寄存器，只有工控机能读；
- 仓库 WMS 用的是定制化 SQL Server 2008，存储过程有三千行，没有任何文档；
- 业务审批的"系统"是微信群消息加人工截图，一旦某个关键人请假，整条流程就断了。

在这种环境里，给员工买几个 SaaS 账号或者写个基于单轮对话的 Agent，两周内必被一线业务人员抛弃。本文不讲概念，只讲方法：如何通过非侵入式数据感知、非标任务 DAG 编排与本地私有大模型网关，真正穿透传统工业软件壁垒，完成智能化改造的最后一公里。

---

## 一、企业大模型落地的最大工程谎言：假装老系统不存在

### 1.1 老旧工业软件的物理壁垒不是"历史包袱"，是现实约束

中国制造与流通企业的 IT 基础设施有一个显著特征：核心业务系统高度定制、深度耦合，迁移成本极高，但因为"还能用"，就一直运转下去。这造成了以下几类工程级壁垒：

**封闭协议与无开放 API**

金蝶 K3 Cloud 之前的旧版本、用友 T6/U8 系列的核心单据接口，大多通过 COM 组件或私有 Socket 协议暴露，没有任何 REST/GraphQL 接口。鼎捷的 ERP 有官方 SDK，但文档完整度不足 30%，且 SDK 的并发安全性极差——在压测中，我们实测过同时发起 8 个写入请求时，系统有 15% 的概率返回死锁异常。

**核心库写入风险极高**

直接向老旧 ERP 的生产订单表写入记录，是一件极其危险的操作。以某华东零部件厂为例，其金蝶系统的生产工单状态机由十七张关联表维护，任何一张表的非法写入都可能导致工单状态不一致，进而触发下游 MES 的异常停线。我们曾见过一次因外部系统写入错误字段，导致整个车间停产四小时的事故。

**并发性能差与事务语义不可控**

老旧 SQL Server 实例通常部署在十年前的物理机上，没有读写分离，主库承载全部 OLTP 压力。任何外部系统如果不经过读写分离就直接并发查询，极容易将主库打满，影响正常业务。

**工控协议的孤岛性**

Modbus/OPC-UA 的数据不会自动流入任何 IT 系统。它们存在于 PLC 的寄存器里，只有工控机上的 SCADA 软件能实时读取。大多数企业没有任何机制将工控数据与 ERP 业务数据关联起来，这两套数据在物理上就是断裂的。

### 1.2 为什么通用 AI 插件在这里毫无生产力价值

通用 AI 插件的架构前提是"系统有标准 API，数据在云端，用户只需要自然语言描述需求"。这三个前提在中国大多数制造企业里全部不成立。

一个典型的失败案例：某厂商为一家零部件企业部署了基于 GPT-4 的 AI 助手，接入了钉钉和企业微信。员工可以用自然语言查询库存。但这个"查询"的实现方式是：每天凌晨将 ERP 数据导出成 Excel，上传到云端向量库，再由大模型检索回答。结果是：数据延迟 24 小时、无法反映实时库存、一旦 Excel 格式变更就整体失效、关键数据（供应商报价、生产成本）都上传到了外部云端。这个方案在三周后被彻底放弃。

根本原因在于：**这类方案没有处理"数据在哪里、以什么格式存在、如何实时感知变更"这三个工程问题，只是在一个不存在的理想接口上叠加了 AI 能力。**

---

## 二、穿透传统工业软件的四步工程法

### 第一步：非侵入式数据感知（Non-invasive CDC & Shadow Tables）

不修改老系统，是铁律。任何需要在老系统里安装 Agent、修改配置或增加触发器的方案，都面临极高的上线风险和长达数月的 IT 审批周期。我们的方法论是：把老系统当作一个只读的物理黑盒，通过日志层感知它的状态变化。

**Binlog 监听（Debezium + Kafka）**

对于 MySQL 作为底层存储的 ERP（部分金蝶、鼎捷版本），可以通过开启 binlog 的方式，用 Debezium 在完全不接触业务库的前提下捕获行级变更事件。Debezium 作为 MySQL 的 replica 从库连接，读取 binlog 流，将 INSERT/UPDATE/DELETE 操作转化为结构化事件，推送至 Kafka Topic。

关键配置点：
- `binlog_format` 必须设置为 `ROW`，否则无法解析列级变更；
- Debezium Connector 使用独立的只读账号，权限严格限定为 `REPLICATION SLAVE, REPLICATION CLIENT`；
- 对高频变更表（如库存流水表、生产报工表）设置单独的 Topic，避免事件混流。

**只读从库订阅**

对于 SQL Server 作为底层存储的 ERP（用友 U8、大多数 WMS），可以配置 SQL Server 的 Always-On 可用性组或简单的日志传送（Log Shipping），搭建只读从库。所有外部查询全部打到从库，主库零压力。

**轻量化虚拟影子表（Shadow Tables）**

针对无法直接读取底层数据库（如部分通过 COM 接口暴露数据的系统），我们在独立的 PostgreSQL 实例中建立影子表，由专属的轻量 Adapter 进程（定时轮询或 Hook 注入）将数据同步至影子表。影子表的 Schema 由我们自定义，字段命名标准化，与老系统原始表结构解耦。

这一步的核心价值：**外部所有系统只与影子表交互，老系统主库对 AI 层完全不可见。**

```

```python
# write_to_file.py - 继续写入剩余内容
remaining_content = """
### 第二步：统一事件中枢与状态机骨架（Event Spine & State Machine）

感知到数据变化只是第一步。原始的 binlog 事件是噪声：它告诉你"哪张表的哪行发生了变化"，但不告诉你"这意味着什么业务含义"。

我们的做法是在 Kafka 之上建立一层 Event Spine（事件脊柱），将离散的表级变更映射为有业务语义的强类型事件流。

**事件类型定义示例（Pydantic Schema）：**

```python
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime
from typing import Optional

class OrderEventType(str, Enum):
    CREATED = "order.created"
    MATERIAL_SHORTAGE = "order.material_shortage"
    PRODUCTION_DELAYED = "order.production_delayed"
    QC_REJECTED = "order.qc_rejected"
    SHIPPED = "order.shipped"

class ProductionOrderEvent(BaseModel):
    event_id: str = Field(..., description="全局唯一事件ID，UUID4")
    event_type: OrderEventType
    order_no: str
    plant_code: str
    product_sku: str
    scheduled_date: datetime
    actual_date: Optional[datetime] = None
    delay_hours: Optional[float] = None
    shortage_materials: Optional[list[str]] = None
    source_system: str = Field(..., description="事件来源：ERP/MES/WMS/MANUAL")
    raw_payload: dict = Field(..., description="原始变更快照，用于审计回溯")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
```

状态机骨架的核心价值在于：**每个业务对象（生产工单、发货单、运价协议）在任意时刻都有一个明确的状态节点，任何 DAG 任务都基于当前状态决定下一步动作，而不是盲目触发。**

状态转换的合法性由预定义的有限状态机（FSM）约束。非法的状态跳转（如工单未经 QC 直接进入发货状态）会被状态机拦截，进入人工审查队列，而不是由大模型自行决策。

### 第三步：Orchestrator-Worker-Evaluator 非标任务 DAG 引擎

这是整个架构的核心，也是我们与市面上大多数"AI Agent 框架"的根本分野。

**为什么抛弃单体 Agent**

单体 Agent 的架构是：接收输入 -> 大模型推理 -> 输出结果（或调用工具） -> 循环。在工业场景里，这个架构有三个致命缺陷：

第一，单点失败。大模型输出错误时，没有校验层，错误会直接传播到下游动作；
第二，状态不透明。中间推理过程无法审计，出了问题无法重现和排查；
第三，无法并行。工业任务常常需要同时查询多个系统（ERP 库存 + MES 工单 + WMS 库位），单体 Agent 的串行调用让延迟线性叠加。

**OWE 三层解耦架构**

我们采用 Orchestrator（编排器）- Worker（执行器）- Evaluator（评估器）的三层解耦结构：

**Orchestrator（编排器）**：负责接收触发事件，根据事件类型和当前状态，展开 DAG 图，确定任务节点的依赖关系与并行分支。Orchestrator 不调用大模型，只做规划。它维护每个 DAG 实例的运行状态，支持状态回溯（Checkpoint），在任意节点失败时可以从上一个成功节点重试。

**Worker（执行器）**：负责执行具体的子任务节点。根据任务复杂度，Worker 可以选择三种执行模式：
- 规则引擎直接处理（无大模型，延迟 < 5ms）；
- 小模型（如本地 Qwen-7B 量化版）处理格式清洗、实体抽取等轻量任务；
- 大模型（如 Qwen-72B 或 GPT-4o）处理复杂策略仲裁、多约束推理。

**Evaluator（评估器）**：对 Worker 的输出进行双重校验。第一层是规则校验（Schema 合规性、业务约束、数字范围检查）；第二层是大模型语义校验（输出是否逻辑自洽、是否存在幻觉特征）。只有两层校验均通过，结果才会进入下游节点。

**调度伪代码（Python / FastAPI + Redis / Celery）：**

```python
# dag_engine/orchestrator.py
import asyncio
import uuid
from typing import Any, Dict, List
from datetime import datetime
import redis.asyncio as aioredis
from celery import Celery
from pydantic import BaseModel

celery_app = Celery("fde_dag", broker="redis://localhost:6379/0")
redis_client = aioredis.from_url("redis://localhost:6379/1")

class DAGTaskNode(BaseModel):
    task_id: str
    task_type: str  # RULE / SMALL_MODEL / LARGE_MODEL
    depends_on: List[str]
    timeout_seconds: int = 30
    retry_limit: int = 2
    human_escalation_on_failure: bool = False

class DAGInstance(BaseModel):
    instance_id: str
    trigger_event: dict
    nodes: List[DAGTaskNode]
    status: str = "PENDING"  # PENDING / RUNNING / PARTIAL_FAIL / SUCCESS / FAILED
    checkpoint: Dict[str, Any] = {}
    created_at: datetime = datetime.utcnow()

class Orchestrator:
    def __init__(self):
        self.dag_registry = {}  # event_type -> DAGTemplate

    async def dispatch(self, event: dict) -> str:
        event_type = event.get("event_type")
        template = self.dag_registry.get(event_type)
        if not template:
            await self._alert_unhandled_event(event)
            return "UNHANDLED"

        instance_id = str(uuid.uuid4())
        dag = DAGInstance(
            instance_id=instance_id,
            trigger_event=event,
            nodes=template.expand(event)
        )

        # 持久化 DAG 实例到 Redis，支持崩溃恢复
        await redis_client.setex(
            f"dag:instance:{instance_id}",
            86400,
            dag.model_dump_json()
        )

        # 按拓扑序异步调度节点
        await self._schedule_ready_nodes(dag)
        return instance_id

    async def _schedule_ready_nodes(self, dag: DAGInstance):
        ready_nodes = [
            node for node in dag.nodes
            if all(
                dag.checkpoint.get(dep) == "SUCCESS"
                for dep in node.depends_on
            )
            and dag.checkpoint.get(node.task_id) is None
        ]

        tasks = [
            self._dispatch_worker_task(dag, node)
            for node in ready_nodes
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _dispatch_worker_task(self, dag: DAGInstance, node: DAGTaskNode):
        try:
            result = await asyncio.wait_for(
                self._invoke_worker(dag, node),
                timeout=node.timeout_seconds
            )
            eval_result = await self._invoke_evaluator(node, result)

            if eval_result.passed:
                dag.checkpoint[node.task_id] = "SUCCESS"
                await self._persist_checkpoint(dag)
                # 继续调度后续节点
                await self._schedule_ready_nodes(dag)
            else:
                await self._handle_evaluator_rejection(dag, node, eval_result)

        except asyncio.TimeoutError:
            await self._handle_timeout(dag, node)
        except Exception as e:
            await self._handle_exception(dag, node, e)

    async def _handle_evaluator_rejection(self, dag, node, eval_result):
        if node.retry_limit > 0:
            node.retry_limit -= 1
            await self._dispatch_worker_task(dag, node)
        elif node.human_escalation_on_failure:
            # 推送至 Human-on-the-loop 异步干预队列
            await redis_client.lpush(
                "queue:human_escalation",
                {
                    "dag_instance_id": dag.instance_id,
                    "failed_node": node.task_id,
                    "eval_reason": eval_result.rejection_reason,
                    "context": dag.trigger_event,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            dag.status = "AWAITING_HUMAN"
            await self._persist_checkpoint(dag)
        else:
            dag.status = "FAILED"
            await self._alert_dag_failure(dag, node)

    async def _alert_unhandled_event(self, event: dict):
        # 钉钉/飞书 Webhook 推送，确保告警不丢失
        alert_payload = {
            "alert_level": "WARN",
            "message": f"Unhandled event type: {event.get('event_type')}",
            "raw_event": event,
            "timestamp": datetime.utcnow().isoformat()
        }
        await redis_client.lpush("queue:alerts", str(alert_payload))
```

**Human-on-the-loop 异步干预队列**是这套架构的关键安全阀。当 Evaluator 连续两次拒绝 Worker 的输出，系统不会自行决策，而是将当前 DAG 实例冻结，将完整上下文推送至业务主管的钉钉/飞书审批界面。人工决策的结果会回写至 DAG checkpoint，引擎从冻结点继续执行。

这保证了：**AI 负责大多数场景的自动化，人负责例外情况的兜底，两者职责边界清晰，不存在 AI 自行处理不确定情况的"灰色地带"。**

### 第四步：本地私有大模型网关（Local Model Gateway）

这是安全与成本的双重核心。

**任务级性价比路由**

并非所有任务都需要大模型。我们将任务按复杂度分级：

| 任务类型 | 示例 | 推荐模型 | 延迟目标 |
|----------|------|----------|----------|
| 格式清洗、实体抽取 | 从备注文本中提取物料编号 | Qwen-1.5B-INT4（本地） | < 100ms |
| 分类判别、简单摘要 | 告警类型分类、工单摘要 | Qwen-7B-INT8（本地） | < 500ms |
| 复杂策略仲裁 | 多约束排产优化、异常根因分析 | Qwen-72B 或 GPT-4o | < 5s |
| 确定性计算 | 库存余量计算、交期推算 | 规则引擎（无大模型） | < 5ms |

网关根据 DAG 节点的任务类型标签，自动路由至对应模型，不需要 Worker 感知底层模型细节。

**敏感字段物理脱敏（PII Sanitization）**

所有进入大模型（尤其是云端模型）的 Prompt，必须经过脱敏层处理。脱敏不是简单的字符串替换，而是基于字段语义的结构化处理：

- 供应商名称 -> `SUPPLIER_<HASH_6>`
- 客户合同金额 -> `<AMOUNT_REDACTED>`
- 员工手机号 -> `<PHONE_REDACTED>`
- 工厂内部物料编号 -> 可选保留（根据企业数据分级策略）

脱敏映射表存储在本地，模型返回结果后，网关自动将占位符替换回原始值，上层应用无感知。

**毫秒级热备熔断**

本地模型节点按主备模式部署。主节点（如 vLLM 推理服务）若连续三次响应超时（默认 30s），熔断器触发，自动切换至备节点。同时推送告警至运维队列，由 DevOps 团队排查主节点故障。熔断恢复后，网关通过流量染色（Canary 1%）验证主节点稳定性，再逐步恢复主节点流量。

**本地日志可追溯审计**

每一次大模型调用，网关记录完整的审计日志：Prompt 原文（脱敏后）、模型版本、Token 消耗、响应时间、输出原文、Evaluator 校验结果。审计日志写入本地 PostgreSQL，保留 180 天，满足大多数制造企业的合规要求。

---

---

## 三、两种路径的硬核对比：传统定制开发 vs. FDE AI 原生事件驱动架构

以下对比基于驻场交付经历的真实数据与工程观察，不来自 PPT 推演，不做平台背书。

| 维度 | 传统定制二次开发 | FDE AI 原生事件驱动架构 |
|------|------------------|------------------------|
| **系统耦合度** | 深度侵入源系统数据库，逻辑与宿主软件表结构强绑定。宿主软件一次版本升级，定制代码往往需要整体重写。耦合是结构性的，不可回避。 | 以影子表与事件总线为隔离层，DAG 节点仅订阅事件流，不直接读写核心业务表。宿主系统升级时，仅需更新事件映射配置，核心推理逻辑完整保留。 |
| **业务适配周期** | 典型周期 6-18 个月。需求调研、原型、测试、回归，每一轮都涉及源系统供应商介入与授权窗口协调，项目管理成本吞噬大量实际开发资源。 | 首轮原型可在 3-6 周内上线影子观测模式。实际推理介入生产决策的时间节点，由业务方自主控制，工程团队不需要等待系统集成审批窗口。 |
| **大模型概率容错** | 传统开发将大模型输出视为确定性接口，直接写入业务流程。一旦模型幻觉触发异常值，没有隔离层，结果直接污染主数据。回滚成本极高。 | DAG 每个节点内置置信度阈值与双向校验逻辑。模型输出不可信时，事件标记为"待人工确认"并挂起，不自动落库。生产主数据零污染是工程红线，不是可选项。 |
| **运维成本** | 运维与二次开发深度捆绑。每次业务规则调整，均需重新走开发-测试-上线流程，且依赖具备宿主系统专项知识的工程师。人员流动直接导致系统黑盒化。 | 规则层与推理层分离。业务规则变更通过 DAG 配置文件完成，不触碰推理代码。运维人员不需要理解大模型内部，只需理解事件流与阈值配置。人员交接成本大幅压缩。 |
| **代码资产归属** | 定制代码通常存放在系统集成商的代码仓库，企业没有独立可运行的版本。一旦甲乙双方关系破裂，企业面临代码与文档双重失控的局面。 | 所有 DAG 定义、事件映射、推理节点代码，完整归属企业私有仓库。私有网关部署在企业自有服务器或本地机房，推理请求不经过任何第三方节点。代码即资产，从第一天起。 |
| **数据合规与主权** | 数据流向依赖集成商的架构设计，企业往往无法精确追踪哪些字段在什么时间被发送至何处。在汽车、军工、医疗等敏感行业，这是实质性合规风险。 | 私有网关作为唯一出口，所有经过网关的数据包均有完整日志留存。敏感字段脱敏在网关层完成，推理模型只见脱敏后的结构化输入。审计链路可复现，监管核查时可提供完整证据链。 |
| **失败降级策略** | 大模型节点故障时，传统架构通常无降级预案，要么整体停服，要么人工紧急切换至手工流程，切换窗口内的数据一致性无保障。 | DAG 每条边均配置超时与降级路由。主推理节点不可达时，自动切换至规则引擎保守模式，业务流程继续运行，仅失去 AI 增强能力，不失去基础功能。降级是设计出来的，不是事故后补救的。 |
| **技术负债累积速度** | 随着业务迭代，定制代码层层叠加在原有补丁之上。三年后的系统，通常没有任何工程师能完整描述数据流向。重构成本与风险均不可估量，企业往往选择继续凑合，直到全面崩溃。 | DAG 的有向无环结构天然限制了逻辑回路的形成。新业务节点以追加而非修改的方式接入，存量节点不受影响。技术负债以可观测的形式累积，不会在某个夜晚突然引爆。 |

---

## 五、现场攻坚实录

### 案例一：华东精密机械零部件——影子表与视觉 DAG 将生产调度延期率压降 75%

#### 现场诊断

这家企业在长三角核心产业带深耕精密零部件超过二十年，主营产品为汽车传动系统关键件与工业机器人结构件。驻场进入时，生产调度系统运行在一套部署于 2014 年的国产 ERP 之上，供应商已停止商业维护，源代码与技术文档均不完整。

调度员每天的核心工作是在 ERP 工单视图与十几张 Excel 交期确认表之间人工比对，识别哪些工单因外协供应链延误或设备临时停机而存在交期风险。这个过程每天消耗两名高级调度员约四小时，且人为判断误差率随排班疲劳度线性上升。订单交期延误直接触发客户罚款条款，年度罚款金额占营收比例已足够让管理层重视。

直接改造 ERP 不可行——供应商已无法提供技术支持，擅自修改数据库结构的风险不可接受。引入新的调度系统则意味着数据迁移与双系统并行期的混乱，这对精密件企业来说是不可接受的生产风险。

#### 工程路径

第一步是构建影子表层。在 ERP 所在服务器同网段部署一台工程机，通过只读数据库镜像权限，将 ERP 的工单表、工序表、设备日历表、外协确认表以每五分钟为周期同步至影子数据库。影子表只读取，从不回写 ERP。这一层的核心工程价值是：ERP 视角下什么都没有发生。

第二步是建立视觉 DAG。调度员原本依赖的是线性工单列表，风险信息淹没在行数中。视觉 DAG 将每个工单的工序链以有向图形式展开，关键路径实时标注，外协节点的延误信号通过事件总线注入对应的 DAG 边，触发颜色与权重变化。调度员的认知负荷从"在列表中找风险"转变为"在图中识别红色路径"，信息密度不变，决策速度显著提升。

第三步是接入推理层。大模型节点订阅两类事件：其一为外协供应商历史交货偏差率超过阈值的预警事件，其二为设备维保日历与当前排班冲突的检测事件。模型输出为结构化的"风险工单列表"与"推荐调度优先级调整方案"，置信度低于 0.82 的建议自动标记为"待人工确认"，不进入自动执行流程。

结果：运行九个月后，生产调度交期延误率从基线的 18.3% 下降至 4.6%，降幅 74.8%。高级调度员从每日四小时的比对工作中释放，转而专注于客户协调与产能规划。ERP 系统全程未被触碰，供应商从未介入。

---

### 案例二：跨国货运与多仓调度——在老旧 TMS 的缝隙里撮合跨时区运力与运价

#### 现场诊断

这家企业承接中欧、中东南亚两条货运走廊的综合物流业务，自有运力与三方承运商混合运营，在国内有七个分拨仓，境外有四个转运节点。核心 TMS（运输管理系统）是一套购于 2011 年的外资软件，本地化深度有限，运价模块与运力调度模块之间的数据交互至今依赖人工导出 CSV 中转。

跨时区运价撮合是最痛的业务场景。境外承运商报价以当地时间工作日为准，国内调度员拿到报价时，往往已经跨过报价有效期或汇率窗口。人工撮合的时间差导致两类损失：一是错过低价窗口导致运输成本上升，二是因确认延迟导致舱位被其他货主占用，转而使用备选方案的成本更高。

TMS 供应商已明确告知：增加 API 接口改造需要报价，周期预计六个月，且不保证数据结构兼容现有报表系统。这是一条死路。

#### 工程路径

私有网关是这个项目的核心基础设施。在企业内网部署一个轻量化的网关节点，承担三项职责：其一，作为境外承运商报价 webhook 的接收终端，统一处理多源格式的报价推送；其二，作为汇率数据的本地缓存层，每十五分钟从公开源拉取并存储在本地，推理时不依赖实时外网请求；其三，作为 TMS 数据的读取代理，通过定时脚本从 TMS 导出的固定格式报表中解析结构化数据，写入内部事件总线。

这里值得单独说明的是第三点。TMS 没有 API，但它有固定格式的定时报表导出功能。将报表导出自动化，并将解析结果转化为事件，是绕过系统封闭性最低摩擦的工程选择。这个方案看起来笨，但它不依赖供应商配合，不修改任何现有系统，且完全可测试。

双向校验逻辑构建在 DAG 的撮合节点之上。大模型接收三类输入：当前待撮合货量与体积、各承运商报价及有效期、历史履约率数据（从影子表中累积）。模型输出推荐撮合方案，校验层随即执行两项检查：其一，推荐方案的总费用是否低于过去三十天同路向均价的 1.15 倍（成本上限红线）；其二，推荐承运商的近九十天准时率是否高于 0.78（质量下限红线）。两项均通过，方案自动推送至调度员审批界面，审批操作为单键确认，不需要重新录入 TMS。若任一校验不通过，事件挂起，系统标注失败原因，不生成推荐。

跨时区问题的解法是将"时间窗口"显式建模为 DAG 中的一个独立节点。该节点在收到报价事件时，立即计算报价的本地剩余有效时长，并将紧迫程度注入事件优先级队列。调度员界面的排序逻辑基于这个优先级，而非报价到达时间。报价快过期的，排在最前面，不管它几点推送过来的。

结果：跨时区运价撮合的平均响应时长从原来的 4.2 小时（人工流程）压缩至 23 分钟（自动推荐加人工确认），舱位失失率下降 61%，同期三方承运商采购成本降低 8.3%。TMS 系统从未被改动，供应商从未介入。

---

## 六、工程总结与核心心法

经历多个产业带的驻场交付后，以下几点判断是从具体问题中蒸馏出来的，不是事先设计好的方法论。

**第一点：不改动宿主系统，是约束，也是护城河。**

进入一个有十年历史的工业软件现场，第一反应应该是"这套系统里沉淀了多少真实业务逻辑"，而不是"这套系统多落后"。侵入式改造的代价不只是技术风险，还有组织风险——系统停机一小时在精密件或货运场景里意味着真实的金融损失与客户信任损耗。影子表与网关的价值，在于将 AI 能力注入系统的方式从"手术"变为"搭载"，宿主系统继续运行，AI 层独立演进。

**第二点：DAG 的边，就是业务规则的可测试形式。**

大模型的输出概率分布与工业业务的确定性需求之间存在根本矛盾，这个矛盾不会因为模型能力提升而消失。DAG 的每条边，实质上是一条业务规则的代码化表达：在什么条件下，这个输出才被允许向下传递。这个设计让业务人员可以直接审查 AI 决策路径，让审计人员可以复现任意历史决策的完整逻辑链，让工程师可以在不理解模型内部的情况下修改规则。可测试的系统才是可维护的系统。

**第三点：私有网关不是性能选项，是主权选项。**

在中国制造业场景里，客户对数据出境的敏感度远高于通常预期。不仅是监管合规压力，还有商业竞争情报保护的现实需求。将推理请求路由通过私有网关，不只是解决延迟问题，更是给企业一个明确的答案：你的生产数据在哪里，走过什么路径，留下什么日志，完全由你自己控制。这个答案在销售层面是必要的，在工程层面是可交付的。

**第四点：AI 不应该是系统的决策者，应该是决策的加速器。**

在所有交付的项目里，最终写入生产数据库的操作，都需要经过人工确认环节。这不是对 AI 能力的不信任，而是对工业系统失效代价的清醒认知。调度员点击确认的那一刻，AI 的推理结果转化为人的决策，责任链条是清晰的。双向校验的价值，正是在模型输出与人的确认之间插入一层可解释的过滤网，让人知道自己在确认什么，而不是在盲签一个黑盒结论。

**第五点：代码写在真实业务现场，数据留在企业自己的服务器上。**

这两句话没有例外，没有"条件允许时"的限定语。脱离真实数据场景写出来的代码，在生产环境里会以各种意想不到的方式失败。数据不在企业自有服务器上，任何关于安全与主权的承诺都是空话。这两点不是工程建议，是工程红线。

---

**出品团队：** AlphaFDE 现场工程交付团队
中国 · 杭州 · 深圳 · 深入产业带驻场

**官方主页：** https://alphafde.cn
**开源与展示仓库：** https://github.com/Alphaxiaoteng/alphafde

---

**工程宣言**

务实、克制、严谨。代码必须写在真实业务现场，数据必须留在企业自己的服务器上。