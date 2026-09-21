# 企业私有大模型网关怎么搭？
## ——为什么不要让员工直接绑个人信用卡刷 API

**作者视角：资深企业 AI 架构师 & FDE（前向部署工程师）**

---

## 前言：我见过太多这种烂摊子

上个月刚交付完一个电商客户的 AI 基础设施审计。进场第一天，财务给我看了一张截图——

**17 个不同的 OpenAI 账户，上个月合计支出 $23,400。**

没有一笔有完整的业务归因。没有任何限流。有三个账户用的是员工个人信用卡，走报销流程。还有一个账户的 API Key 被提交进了公司的 GitHub 仓库，存活了整整 11 个月。

这不是个例。这是行业常态。

你公司里现在大概率也在发生同款事故，只是还没人统计清楚。

---

## 第一章：失控是怎么发生的

### 1.1 账单碎片化：每个业务小组都在重复造轮子

电商公司的组织结构通常是这样的：

- **内容团队**：用 GPT-4o 写商品详情页、直播脚本
- **客服团队**：用 Claude 做差评分析、FAQ 生成
- **运营团队**：用 Midjourney/DALL-E 做主图素材
- **数据团队**：用 GPT-4 做竞品价格情报提取

四个团队，四套账户体系，四套提示词管理方案，四套没有人维护的 Python 脚本。

当我问每个团队"你们每月大概花多少钱"，得到的答案误差率通常超过 300%。因为没人算过。

账单碎片化的真实成本不只是多付了钱——更严重的问题是**没有任何数据可以回答"AI 投入的 ROI 是多少"这个老板会问的问题**。你怎么证明这 $23,400 值？

### 1.2 供应商依赖：某家大模型半夜宕机，你的自动化流水线全线趴下

2024 年 11 月，OpenAI 那次大规模服务中断，持续了将近 4 小时。

我有一个客户，他们的直播选品脚本生成、商品标题批量优化、客服工单分类，全部硬编码打向 `api.openai.com`。那 4 小时，整个内容生产线停转。

事后复盘的时候，CTO 问了一个问题："我们有没有备份？"

答案是：没有。

不是技术团队懒，而是**从来没有人把"多供应商容灾"列进需求**。因为大家习惯性地觉得 OpenAI 是基础设施，就像 AWS S3 一样稳。

但 S3 真的比 OpenAI 的模型服务稳，这不是一个数量级的东西。

### 1.3 数据泄露：你知道你的员工在发什么出去吗？

这是最危险也是最被轻视的问题。

我在那家电商客户里做数据流审计的时候，发现了以下几类数据在没有任何脱敏处理的情况下，直接打包进 prompt 发往外部 API：

- **商品主图原始文件**（包含未发布的新款 SKU）
- **直播脚本初稿**（含竞品价格策略、限时折扣点位）
- **平台店铺 Cookie 字符串**（某员工让 GPT 帮他"分析接口返回数据"）
- **供应商联系方式与账期条款**

最后一条让法务当场变色。这些数据发出去之后，你对它的去向没有任何控制权。你不知道它有没有被用于训练，不知道有没有被人工审核，你唯一能确定的是——**那个 HTTPS 连接建立的那一刻，数据就不再完全属于你了**。

GDPR、国内的《数据安全法》、《个人信息保护法》，随便哪一条对应到这个场景，合规风险都不小。

---

## 第二章：为什么网关是正确答案

解法不是"告诉员工要小心"。人不可靠，流程才可靠。

**正确的工程解法是：在你自己的服务器上搭一层私有模型网关，让所有 AI 请求都经过这一层，然后再转发出去。**

员工端看到的是一个统一的内网 API 地址。他不知道、也不需要知道背后调的是 GPT-4o、Claude 3.5、还是 Gemini 1.5 Pro。

```
员工工具 / 业务脚本
        ↓
   [私有模型网关]  ←── 你能控制的那一层
        ↓
 GPT-4o / Claude / Gemini / 私有化部署模型
```

这一层能做什么？全部列出来：

| 能力 | 实现方式 |
|------|------|
| 统一账单归因 | 按业务标签记录每一笔 token 消耗 |
| 敏感数据脱敏 | 请求进网关时过正则/NER/规则引擎 |
| 任务级模型路由 | 按任务类型分发最性价比模型 |
| 多供应商熔断热备 | 主力模型异常时秒级切换 |
| 统一速率限制 | 防止某个脚本失控烧光预算 |
| 审计日志 | 每一条请求可溯源到人和业务 |

---

## 第三章：工程实现——从零搭一个可用的私有网关

### 3.1 技术选型：为什么推荐 Fastify（Node.js）

如果你的团队是前端/全栈背景，**Fastify** 是最快能跑起来的选择：

- 吞吐量比 Express 高 2-3 倍，足够应付企业内部流量
- 插件生态成熟，日志、schema validation 开箱即用
- 异步流式响应（`Transfer-Encoding: chunked`）处理 SSE 无压力

如果是后端团队，**Go（用 Gin 或 Fiber）** 更适合高并发场景，内存占用极低，部署一个 8MB 的二进制就完事。

Python（FastAPI）适合数据科学背景的团队，但在高并发流式场景下需要额外调优。

**以下示例用 Fastify 实现核心骨架：**

### 3.2 核心结构

```
enterprise-ai-gateway/
├── src/
│   ├── server.js           # Fastify 主服务
│   ├── routes/
│   │   └── chat.js         # /v1/chat/completions 路由
│   ├── middleware/
│   │   ├── auth.js         # 内部 API Key 验证
│   │   ├── sanitizer.js    # 敏感数据脱敏
│   │   └── ratelimit.js    # 速率限制
│   ├── router/
│   │   └── modelRouter.js  # 任务级模型路由逻辑
│   ├── providers/
│   │   ├── openai.js
│   │   ├── anthropic.js
│   │   └── gemini.js
│   ├── circuit/
│   │   └── breaker.js      # 熔断器
│   └── logger/
│       └── audit.js        # 审计日志
├── config/
│   └── routing-rules.yaml  # 路由规则，业务侧可自助配置
├── docker-compose.yml
└── Dockerfile
```

### 3.3 统一 OpenAI 格式接口抽象

这是网关最核心的设计原则：**对内暴露标准 OpenAI 兼容接口，对外适配各家供应商的私有格式。**

这样做的好处是——员工用的任何工具，只要支持"自定义 API 地址"，改一行配置就能接入网关，不用改代码。

```javascript
// src/routes/chat.js
import fp from 'fastify-plugin'
import { modelRouter } from '../router/modelRouter.js'
import { sanitizeRequest } from '../middleware/sanitizer.js'
import { auditLog } from '../logger/audit.js'

export default fp(async (fastify) => {
  fastify.post('/v1/chat/completions', {
    schema: {
      headers: {
        type: 'object',
        required: ['authorization'],
        properties: {
          authorization: { type: 'string' }
        }
      }
    }
  }, async (request, reply) => {
    const startTime = Date.now()
    const { body, headers } = request
    
    // 1. 身份验证 & 业务标签提取
    const identity = request.identity // 由 auth middleware 注入
    
    // 2. 敏感数据脱敏（进网关就脱，不等转发时才处理）
    const sanitizedBody = await sanitizeRequest(body, identity.businessUnit)
    
    // 3. 模型路由决策
    const { provider, model, reason } = modelRouter.route({
      taskType: sanitizedBody.metadata?.taskType || 'general',
      estimatedTokens: estimateTokens(sanitizedBody.messages),
      businessUnit: identity.businessUnit,
      priority: sanitizedBody.metadata?.priority || 'standard'
    })
    
    fastify.log.info({ 
      msg: 'Route decision',
      provider, model, reason,
      user: identity.userId 
    })
    
    // 4. 流式响应处理
    if (sanitizedBody.stream) {
      reply.raw.setHeader('Content-Type', 'text/event-stream')
      reply.raw.setHeader('Cache-Control', 'no-cache')
      
      const stream = await provider.streamChat(sanitizedBody, model)
      
      for await (const chunk of stream) {
        reply.raw.write(`data: ${JSON.stringify(chunk)}\n\n`)
      }
      
      reply.raw.write('data: [DONE]\n\n')
      reply.raw.end()
    } else {
      const response = await provider.chat(sanitizedBody, model)
      
      // 5. 审计日志（异步，不阻塞响应）
      auditLog.record({
        userId: identity.userId,
        businessUnit: identity.businessUnit,
        taskType: sanitizedBody.metadata?.taskType,
        provider,
        model,
        promptTokens: response.usage?.prompt_tokens,
        completionTokens: response.usage?.completion_tokens,
        latencyMs: Date.now() - startTime,
        sanitizedFields: sanitizedBody._sanitizedFields
      })
      
      return response
    }
  })
})
```

### 3.4 敏感数据脱敏：不是可选项，是默认行为

脱敏规则分三层，层层递进：

**第一层：结构化字段匹配（最快，规则引擎）**

```javascript
// src/middleware/sanitizer.js
const SENSITIVE_PATTERNS = {
  // 中国手机号
  phone: /1[3-9]\d{9}/g,
  // 身份证号
  idCard: /[1-9]\d{5}(18|19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[\dXx]/g,
  // Cookie 字符串（粗匹配）
  cookie: /(?:cookie|Cookie)[\s:=]+[^\n]{20,}/g,
  // API Key 格式
  apiKey: /(?:sk-|api[_-]?key[\s:=]+)[A-Za-z0-9\-_]{20,}/gi,
  // 银行卡号
  bankCard: /\b[1-9]\d{15,18}\b/g,
  // 邮箱
  email: /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g
}

export async function sanitizeRequest(body, businessUnit) {
  const sanitizedFields = []
  
  const processText = (text) => {
    let processed = text
    
    for (const [type, pattern] of Object.entries(SENSITIVE_PATTERNS)) {
      const matches = processed.match(pattern)
      if (matches) {
        sanitizedFields.push({ type, count: matches.length })
        processed = processed.replace(pattern, `[REDACTED:${type.toUpperCase()}]`)
      }
    }
    
    return processed
  }
  
  // 递归处理 messages 数组
  const sanitizedMessages = body.messages.map(msg => ({
    ...msg,
    content: typeof msg.content === 'string' 
      ? processText(msg.content)
      : msg.content.map(part => 
          part.type === 'text' 
            ? { ...part, text: processText(part.text) }
            : part  // 图片 base64 暂不处理，由图像内容策略单独管控
        )
  }))
  
  return {
    ...body,
    messages: sanitizedMessages,
    _sanitizedFields: sanitizedFields
  }
}
```

**第二层：NER 实体识别（中等精度，异步）**

对于高安全级别的业务单元，在规则引擎之后再跑一遍 NER，识别人名、机构名、地址等结构化实体。

可以用本地部署