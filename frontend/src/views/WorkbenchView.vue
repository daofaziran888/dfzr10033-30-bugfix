<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <section v-if="meta.key === 'data_lake_read'" class="diff-panel" data-testid="lake-diff-panel">
      <header class="diff-panel-head">
        <h3>来源实际字段差异</h3>
        <div class="page-actions">
          <label class="filter-item">
            <span>来源id</span>
            <input v-model="diffSourceId" data-testid="diff-source-id" placeholder="如 1" />
          </label>
          <button class="btn" data-testid="load-diffs" type="button" @click="loadDiffs">查询来源差异</button>
        </div>
      </header>
      <p v-if="diffError" class="error-text">{{ diffError }}</p>
      <table class="data-table">
        <thead>
          <tr>
            <th>来源差异编号</th>
            <th>扫描批次</th>
            <th>新增字段</th>
            <th>移除字段</th>
            <th>湖仓记录</th>
            <th>湖仓扫描批次</th>
            <th>实际字段差异</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in sourceDiffs" :key="item.来源差异编号" :data-testid="`diff-${item.来源差异编号}`">
            <td>{{ item.来源差异编号 }}</td>
            <td>{{ item.扫描批次 }}</td>
            <td>{{ item.新增字段 || '—' }}</td>
            <td>{{ item.移除字段 || '—' }}</td>
            <td>{{ item.湖仓记录?.路径编号 ?? '—' }}</td>
            <td>{{ item.湖仓记录?.扫描批次 ?? '—' }}</td>
            <td class="mono">{{ item.湖仓记录?.实际字段差异 ?? '—' }}</td>
          </tr>
          <tr v-if="!sourceDiffs.length">
            <td colspan="7" class="empty-state">该来源暂无已确认的实际字段差异</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  listSourceSchemaDiffs,
  moduleMeta,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow } from '@/data/types'
import type { SourceSchemaDiff } from '@/api/backend-service'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const diffSourceId = ref('')
const sourceDiffs = ref<SourceSchemaDiff[]>([])
const diffError = ref('')
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
    if (meta.key === 'data_lake_read') await refreshLakeDiffs()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

async function refreshLakeDiffs() {
  // 湖仓列表默认展示当前路径关联来源的差异；未填来源时不发请求。
  if (!diffSourceId.value.trim()) return
  await loadDiffs()
}

async function loadDiffs() {
  diffError.value = ''
  const sourceId = Number(diffSourceId.value)
  if (!Number.isInteger(sourceId) || sourceId <= 0) {
    diffError.value = '请输入有效的来源id'
    return
  }
  try {
    const payload = await listSourceSchemaDiffs(sourceId)
    sourceDiffs.value = payload.items
  } catch (error) {
    diffError.value = error instanceof Error ? error.message : '来源差异清单读取失败'
  }
}

onMounted(reload)
</script>
