<template>
  <section class="page" data-module="hotspot">
    <header class="page-head">
      <div>
        <h2>链路热点分析</h2>
        <p class="page-desc">
          按统计窗口汇总同一批次各节点耗时、吞吐与错误占比，定位对总耗时影响最大的节点及受影响下游路径，
          并给出暂停、限流或继续观察的处置建议。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" :disabled="loading" @click="runAnalysis">
          {{ loading ? '分析计算中…' : '发起批次分析' }}
        </button>
        <button class="btn" type="button" @click="reloadAll">刷新</button>
      </div>
    </header>

    <!-- 判定口径 -->
    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">统计窗口</span>
        <strong class="stat-value">{{ config.窗口分钟 ?? 15 }} 分钟（滚动窗口）</strong>
      </article>
      <article v-for="(value, key) in config.阈值" :key="key" class="stat-card">
        <span class="stat-label">{{ thresholdLabels[key] ?? key }}</span>
        <strong class="stat-value">{{ value }}</strong>
      </article>
    </div>

    <div v-if="message" :class="['notice', messageOk ? 'ok' : 'error-text']">{{ message }}</div>

    <!-- 最新批次结论 -->
    <template v-if="latest">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">最新批次</span>
          <strong class="stat-value">{{ latest.批次编号 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">结论版本</span>
          <strong class="stat-value">v{{ latest.版本 }}（同批次只保留最新）</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">窗口内运行记录</span>
          <strong class="stat-value">{{ latest.窗口内运行记录数 }} 条</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">热点节点</span>
          <strong class="stat-value">{{ latest.处理优先级.length }} 个</strong>
        </article>
      </div>

      <!-- 处理优先级看板 -->
      <h3 class="block-title">处理优先级看板</h3>
      <table v-if="latest.处理优先级.length" class="data-table">
        <thead>
          <tr>
            <th>优先级</th><th>链路</th><th>热点节点</th><th>建议动作</th>
            <th>影响分</th><th>受影响下游路径</th><th>处置</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in latest.处理优先级" :key="item.节点编号">
            <td>P{{ item.全局优先级 }}</td>
            <td>{{ item.链路编号 }}</td>
            <td>{{ item.节点编号 }}</td>
            <td><span :class="['tag', actionClass(item.建议动作)]">{{ item.建议动作 }}</span></td>
            <td>{{ item.影响分 }}</td>
            <td>{{ item.受影响下游.length ? item.受影响下游.join(' → ') : '链路末端，无下游' }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="apply(item, '暂停')">暂停</button>
              <button class="link" type="button" @click="apply(item, '限流')">限流</button>
              <button class="link" type="button" @click="apply(item, '观察')">观察</button>
              <button class="link" type="button" @click="resume(item.节点编号)">恢复续跑</button>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-else class="empty-box">
        <p class="empty-title">暂无热点节点</p>
        <p class="empty-desc">批次 {{ latest.批次编号 }} 窗口内全部链路运行平稳，建议继续观察，下一批次自动复核。</p>
      </div>

      <!-- 按链路展示前五节点与受影响路径 -->
      <h3 class="block-title">分链路节点明细（每链路取耗时前五）</h3>
      <div v-for="chain in latest.链路结论" :key="chain.链路编号" class="chain-card">
        <div class="chain-head">
          <strong>{{ chain.链路编号 }}</strong>
          <span class="muted">
            节点 {{ chain.节点数 }} 个 · 窗口样本 {{ chain.窗口样本数 }} 条 ·
            链路总耗时 {{ chain.链路窗口总耗时毫秒 }}ms · 错误 {{ chain.链路窗口错误总数 }} 个
          </span>
        </div>
        <table v-if="!chain.无异常" class="data-table">
          <thead>
            <tr>
              <th>排名</th><th>节点编号</th><th>平均耗时(ms)</th><th>耗时占比</th>
              <th>平均吞吐(条/分)</th><th>窗口错误</th><th>错误占比</th><th>受影响路径</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(metric, idx) in topFive(chain)" :key="metric.节点编号"
                :class="{ hot: isHot(chain, metric.节点编号) }">
              <td>{{ idx + 1 }}</td>
              <td>{{ metric.节点编号 }}<span v-if="isHot(chain, metric.节点编号)" class="hot-flag">热点</span></td>
              <td>{{ metric.平均耗时毫秒 }}</td>
              <td>{{ pct(metric.耗时占比) }}</td>
              <td>{{ metric.平均吞吐条分 }}</td>
              <td>{{ metric.窗口错误数 }}</td>
              <td>{{ pct(metric.错误占比) }}</td>
              <td>{{ hotspotPath(chain, metric.节点编号) }}</td>
            </tr>
          </tbody>
        </table>
        <div v-else class="empty-box small">
          <p class="empty-title">暂无异常</p>
          <p class="empty-desc">{{ chain.结论 }}</p>
        </div>
      </div>
    </template>
    <div v-else class="empty-box">
      <p class="empty-title">暂无热点分析结论</p>
      <p class="empty-desc">点击右上角「发起批次分析」，系统将按 {{ config.窗口分钟 ?? 15 }} 分钟窗口汇总节点指标。</p>
    </div>

    <!-- 传播待办：下游等待清单 + 待限流清单 -->
    <h3 class="block-title">
      传播待办区
      <span class="muted">（下游节点等待清单，含待限流清单）</span>
    </h3>
    <div class="filter-bar">
      <button
        v-for="opt in todoFilters"
        :key="opt.value"
        :class="['btn', todoType === opt.value ? 'primary' : 'ghost']"
        type="button"
        @click="todoType = opt.value"
      >
        {{ opt.label }}
      </button>
    </div>
    <table v-if="todos.length" class="data-table">
      <thead>
        <tr><th>批次</th><th>节点</th><th>待办类型</th><th>来源热点</th><th>状态</th><th>说明</th></tr>
      </thead>
      <tbody>
        <tr v-for="todo in todos" :key="String(todo.id)">
          <td>{{ todo.批次编号 }}</td>
          <td>{{ todo.节点编号 }}</td>
          <td><span :class="['tag', todo.待办类型 === '待限流' ? 'warn' : 'wait']">{{ todo.待办类型 }}</span></td>
          <td>{{ todo.来源节点 }}</td>
          <td>{{ todo.处理状态 }}</td>
          <td>{{ todo.说明 }}</td>
        </tr>
      </tbody>
    </table>
    <div v-else class="empty-box small">
      <p class="empty-title">暂无待办</p>
      <p class="empty-desc">当前没有等待恢复或待限流的下游节点。</p>
    </div>

    <!-- 历史运行记录：原始字段始终可查询，含窗口外记录 -->
    <h3 class="block-title">链路历史运行记录（窗口外记录同样可查询）</h3>
    <form class="filter-bar" @submit.prevent="reloadRuns">
      <label class="filter-item">
        <span>节点编号</span>
        <input v-model="runFilter" placeholder="如 PIPE-0002，留空查全部" />
      </label>
      <button class="btn" type="submit">查询</button>
    </form>
    <table class="data-table">
      <thead>
        <tr><th>节点编号</th><th>开始时间</th><th>耗时(ms)</th><th>吞吐(条/分)</th><th>错误数</th><th>是否窗口内</th></tr>
      </thead>
      <tbody>
        <tr v-for="run in runs" :key="String(run.id)">
          <td>{{ run.节点编号 }}</td>
          <td>{{ run.开始时间 }}</td>
          <td>{{ run.耗时毫秒 }}</td>
          <td>{{ run.吞吐条分 }}</td>
          <td>{{ run.错误数 }}</td>
          <td><span :class="['tag', run.窗口内 ? 'ok' : 'muted-tag']">{{ run.窗口内 ? '窗口内' : '窗口外' }}</span></td>
        </tr>
        <tr v-if="!runs.length">
          <td colspan="6" class="empty-state">暂无历史运行记录</td>
        </tr>
      </tbody>
    </table>

    <!-- 历史分析版本 -->
    <h3 class="block-title">同批次历史结论（重复分析保留审计轨迹）</h3>
    <table v-if="history.length" class="data-table">
      <thead><tr><th>批次</th><th>版本</th><th>分析时间</th><th>窗口</th><th>结论</th></tr></thead>
      <tbody>
        <tr v-for="item in history" :key="String(item.id)">
          <td>{{ item.批次编号 }}</td>
          <td>v{{ item.版本 }}</td>
          <td>{{ item.分析时间 }}</td>
          <td>{{ item.窗口开始 }} ~ {{ item.窗口结束 }}</td>
          <td>{{ item.结论 }}</td>
        </tr>
      </tbody>
    </table>
    <div v-else class="empty-box small">
      <p class="empty-title">暂无历史版本</p>
      <p class="empty-desc">同一批次发起第二次分析后，旧结论会归档到这里。</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type PriorityItem = {
  全局优先级: number
  链路编号: string
  节点编号: string
  建议动作: string
  影响分: number
  受影响下游: string[]
}

type NodeMetric = {
  节点编号: string
  样本数: number
  窗口总耗时毫秒: number
  平均耗时毫秒: number
  平均吞吐条分: number
  窗口错误数: number
  耗时占比: number
  错误占比: number
}

type HotspotDetail = NodeMetric & { 受影响路径: string[] }

type Chain = {
  链路编号: string
  节点数: number
  窗口样本数: number
  链路窗口总耗时毫秒: number
  链路窗口错误总数: number
  节点指标: NodeMetric[]
  热点节点: HotspotDetail[]
  无异常: boolean
  结论: string
}

type Analysis = {
  id: number
  批次编号: string
  版本: number
  窗口开始: string
  窗口结束: string
  窗口内运行记录数: number
  处理优先级: PriorityItem[]
  链路结论: Chain[]
  无异常: boolean
  结论: string
  分析时间: string
}

type Todo = {
  id: number
  批次编号: string
  节点编号: string
  待办类型: string
  来源节点: string
  处理状态: string
  说明: string
}

type RunRow = {
  id: number
  节点编号: string
  开始时间: string
  耗时毫秒: number
  吞吐条分: number
  错误数: number
  窗口内: boolean
}

const config = ref<Record<string, any>>({ 阈值: {} })
const analyses = ref<Analysis[]>([])
const todos = ref<Todo[]>([])
const runs = ref<RunRow[]>([])
const history = ref<Analysis[]>([])
const loading = ref(false)
const message = ref('')
const messageOk = ref(true)
const todoType = ref<string>('')
const runFilter = ref('')

const todoFilters = [
  { label: '全部', value: '' },
  { label: '待恢复等待', value: '待恢复等待' },
  { label: '待限流清单', value: '待限流' },
]

const thresholdLabels: Record<string, string> = {
  平均耗时毫秒: '耗时阈值(ms)',
  窗口错误数: '错误数阈值',
  错误占比: '错误占比阈值',
  低吞吐条分: '低吞吐阈值(条/分)',
}

const latest = computed<Analysis | null>(() =>
  analyses.value.length ? analyses.value[analyses.value.length - 1] : null,
)

function pct(value: number): string {
  return `${(value * 100).toFixed(1)}%`
}

function topFive(chain: Chain): NodeMetric[] {
  return [...chain.节点指标]
    .sort((a, b) => b.窗口总耗时毫秒! - a.窗口总耗时毫秒! || b.平均耗时毫秒 - a.平均耗时毫秒)
    .slice(0, 5)
}

function isHot(chain: Chain, code: string): boolean {
  return chain.热点节点.some((item) => item.节点编号 === code)
}

function hotspotPath(chain: Chain, code: string): string {
  const target = chain.热点节点.find((item) => item.节点编号 === code)
  return target ? target.受影响路径.join(' → ') : '—'
}

function actionClass(action: string): string {
  if (action === '暂停') return 'danger'
  if (action === '限流') return 'warn'
  return 'wait'
}

function notify(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

async function runAnalysis() {
  loading.value = true
  notify('')
  try {
    const response = await request('/api/hotspot/analyses', {
      method: 'POST',
      body: JSON.stringify({}),
    })
    if (response.status === 409) {
      const detail = await response.json().catch(() => ({ detail: '同批次分析正在运行' }))
      notify(detail.detail ?? '同批次已有分析在运行，并发只接受一次生效', false)
    } else if (!response.ok) {
      throw new Error(`接口返回 ${response.status}`)
    } else {
      const data = (await response.json()) as Analysis
      notify(`批次 ${data.批次编号} 分析完成：${data.结论}`)
    }
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '批次分析失败', false)
  } finally {
    loading.value = false
  }
}

async function apply(item: PriorityItem, action: string) {
  if (!latest.value) return
  notify('')
  try {
    const response = await request('/api/hotspot/actions', {
      method: 'POST',
      body: JSON.stringify({
        node_code: item.节点编号,
        action,
        batch_id: latest.value.批次编号,
      }),
    })
    const payload = await response.json()
    notify(payload.message, Boolean(payload.ok))
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '处置动作未生效', false)
  }
}

async function resume(code: string) {
  notify('')
  try {
    const response = await request('/api/hotspot/resume', {
      method: 'POST',
      body: JSON.stringify({ node_code: code }),
    })
    const payload = await response.json()
    notify(payload.message, Boolean(payload.ok))
    await reloadAll()
  } catch (error) {
    notify(error instanceof Error ? error.message : '恢复操作失败', false)
  }
}

async function reloadAll() {
  await Promise.all([loadConfig(), loadAnalyses(), loadTodos(), loadRuns(), loadHistory()])
}

async function loadConfig() {
  const response = await request('/api/hotspot/config')
  if (response.ok) config.value = await response.json()
}

async function loadAnalyses() {
  const response = await request('/api/hotspot/analyses')
  if (response.ok) {
    const payload = await response.json()
    analyses.value = (payload.items ?? []) as Analysis[]
  }
}

async function loadTodos() {
  const query = todoType.value ? `?todo_type=${encodeURIComponent(todoType.value)}` : ''
  const response = await request(`/api/hotspot/todos${query}`)
  if (response.ok) {
    const payload = await response.json()
    todos.value = (payload.items ?? []) as Todo[]
  }
}

async function reloadRuns() {
  await loadRuns()
}

async function loadRuns() {
  const query = runFilter.value ? `?node_code=${encodeURIComponent(runFilter.value)}` : ''
  const response = await request(`/api/hotspot/runs${query}`)
  if (response.ok) {
    const payload = await response.json()
    runs.value = (payload.items ?? []) as RunRow[]
  }
}

async function loadHistory() {
  const response = await request('/api/hotspot/analyses/history')
  if (response.ok) {
    const payload = await response.json()
    history.value = (payload.items ?? []) as Analysis[]
  }
}

onMounted(reloadAll)
</script>

<style scoped>
.block-title {
  margin: 20px 0 8px;
  font-size: 15px;
}
.notice {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 12px;
  margin-bottom: 12px;
  font-size: 13px;
}
.notice.ok {
  color: #067647;
  border-color: #abefc6;
  background: #ecfdf3;
}
.chain-card {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 12px;
  margin-bottom: 14px;
}
.chain-head {
  display: flex;
  gap: 10px;
  align-items: baseline;
  margin-bottom: 8px;
}
.muted {
  color: var(--muted);
  font-size: 12px;
  font-weight: normal;
}
.tag {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 10px;
  font-size: 12px;
}
.tag.danger {
  background: #fef3f2;
  color: #b42318;
}
.tag.warn {
  background: #fffaeb;
  color: #b54708;
}
.tag.wait {
  background: #eff8ff;
  color: #175cd3;
}
.tag.ok {
  background: #ecfdf3;
  color: #067647;
}
.tag.muted-tag {
  background: #f2f4f7;
  color: var(--muted);
}
tr.hot {
  background: #fffbfa;
}
.hot-flag {
  margin-left: 6px;
  padding: 0 6px;
  border-radius: 8px;
  font-size: 11px;
  background: #fef3f2;
  color: #b42318;
}
.empty-box {
  background: #fff;
  border: 1px dashed var(--border);
  border-radius: 8px;
  padding: 28px 16px;
  text-align: center;
}
.empty-box.small {
  padding: 16px;
}
.empty-title {
  margin: 0 0 4px;
  font-weight: 600;
}
.empty-desc {
  margin: 0;
  color: var(--muted);
  font-size: 13px;
}
</style>
