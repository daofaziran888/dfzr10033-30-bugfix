import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type SourceSchemaDiff = {
  来源差异编号: string
  来源编号?: string | null
  source_id: number
  扫描批次: string
  新增字段: string
  移除字段: string
  新增字段数: number
  移除字段数: number
  变化摘要: string
  差异指纹: string
  湖仓记录id?: number | null
  湖仓差异状态?: string
  湖仓记录?: {
    id: number
    路径编号: string
    存储位置: string
    扫描批次: string
    实际字段差异: string
    Schema推导: string
    最后扫描: string
    status: string
  } | null
}

export type SourceSchemaDiffResult = {
  ok: boolean
  source_id: number
  source_no?: string
  items: SourceSchemaDiff[]
  total: number
}

export async function listSourceSchemaDiffs(sourceId: number): Promise<SourceSchemaDiffResult> {
  const response = await request(`/api/pipelines/source-schema-diffs?source_id=${sourceId}`)
  if (!response.ok) throw new Error('来源差异清单读取失败')
  return await response.json()
}
