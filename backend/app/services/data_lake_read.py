"""湖仓读取领域服务：数据湖路径注册、Schema自动推导、分区发现与差异追踪。"""
from __future__ import annotations

import random
import re
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.schema_diff import field_diff, scan_guard, split_fields, stable_json
from app.store import store


class DataLakeReadService(BaseModuleService):
    MODULE = "data_lake_read"
    ENTITY = "湖仓路径"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["路径编号", "存储位置", "文件格式", "分区模式", "Schema推导", "文件列表", "最后扫描", "数据量"]
    REQUIRED_FIELDS = ["路径编号", "存储位置", "文件格式"]
    STATUS_ORDER = ["待扫描", "已注册", "Schema变更", "已停用"]
    ACTION_RULES = {"启动扫描": "已注册", "注册路径": "Schema变更", "停用路径": "已停用"}
    NEGATIVE_ACTIONS = ["停用路径"]

    def record_source_field_diff(
        self, diff_row: dict[str, Any], source_entry: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """把来源已确认的字段差异登记为湖仓实际差异记录。

        只消费来源差异清单里已经落定的口径（探测依据 + 单一模型版本映射），
        不重新探测、不重新读模型，因此不会与来源侧的另一轮扫描混用定义。
        同一来源同一差异指纹只登记一次，重复扫描不会新增批次。
        """
        source_id = diff_row.get("source_id")
        if source_id in (None, ""):
            return None
        source_id = int(source_id)
        diff_key = str(diff_row.get("差异指纹") or diff_row.get("来源差异编号") or "")
        batch = str(diff_row.get("扫描批次") or "")

        # 与来源侧扫描共用同一把锁：查重和写入必须原子，否则两个批次会各写一条。
        with scan_guard():
            rows = store.rows(self.MODULE)
            for row in rows:
                if (
                    row.get("文件格式") == "schema_change"
                    and str(row.get("存储位置")) == f"source_registry/{source_id}"
                    and (str(row.get("差异指纹") or "") == diff_key
                         or (batch and str(row.get("扫描批次") or "") == batch))
                ):
                    # 已登记过同一差异：补齐引用即可，不产生第二条实际差异记录。
                    _ensure_diff_fields(row, diff_row)
                    return row

            record_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            additions = diff_row.get("新增字段", "")
            removals = diff_row.get("移除字段", "")
            record = {
                "id": record_id,
                "路径编号": f"DATA-SC{source_id:04d}-{record_id:04d}",
                "存储位置": f"source_registry/{source_id}",
                "文件格式": "schema_change",
                "分区模式": "N/A",
                "Schema推导": diff_row.get("变化摘要") or f"+{additions} / -{removals}",
                "实际字段差异": stable_json({
                    "新增字段": _as_list(additions),
                    "移除字段": _as_list(removals),
                    "变化摘要": diff_row.get("变化摘要"),
                }),
                "来源差异编号": diff_row.get("来源差异编号"),
                "来源差异id": diff_row.get("id"),
                "source_id": source_id,
                "来源编号": diff_row.get("来源编号")
                or (source_entry or {}).get("源编号"),
                "扫描批次": batch,
                "差异指纹": diff_key,
                "差异依据": diff_row.get("差异依据"),
                "文件列表": "",
                "最后扫描": time.strftime("%Y-%m-%d %H:%M:%S"),
                "数据量": str(diff_row.get("新增字段数", 0) + diff_row.get("移除字段数", 0)),
                "status": "Schema变更",
                "pending": True,
                "abnormal": bool(diff_row.get("移除字段数")),
            }
            rows.append(record)
            _write_source_diff_audit(record)
            return record

    def list_lake_diff_records(self, source_id: int | None = None) -> list[dict[str, Any]]:
        """湖仓实际字段差异记录，供来源差异清单关联核对。"""
        return list_lake_diff_records(source_id)

    def scan_and_register(self, path_id: int) -> dict[str, Any]:
        """扫描湖仓路径：写入 source_registry、semantic_model、query_audit、data_contract。"""
        path_entry = store.find(self.MODULE, path_id)
        if path_entry is None:
            return {"ok": False, "message": "湖仓路径不存在"}

        storage = path_entry.get("存储位置", "")
        fmt = path_entry.get("文件格式", "parquet")
        partition = path_entry.get("分区模式", "dt")

        # 模拟扫描：生成文件列表和Schema
        files = [f"{storage}/dt={dt}/part-{i:05d}.{fmt}"
                 for dt in ["2026-11-01", "2026-11-02", "2026-11-03"]
                 for i in range(random.randint(1, 10))]

        schema = _derive_schema(fmt)
        data_size = random.randint(100 * 1024, 1024 * 1024 * 1024)  # 1GB range

        # 更新路径信息
        path_entry["文件列表"] = ", ".join(files[:5]) + f"... ({len(files)} files)"
        path_entry["Schema推导"] = ", ".join(schema)
        path_entry["最后扫描"] = time.strftime("%Y-%m-%d %H:%M:%S")
        path_entry["数据量"] = f"{data_size / (1024*1024):.0f}MB"
        path_entry["status"] = "已注册"
        path_entry["pending"] = False

        # 注册为数据源
        source_id = _register_as_source(storage, fmt, schema)

        # 创建语义模型
        _create_semantic_model_for_lake(source_id, storage, schema)

        # 写入审计
        _audit_lake_scan(path_id, storage, len(files))

        return {
            "ok": True,
            "path_id": path_id,
            "storage": storage,
            "files_found": len(files),
            "schema_fields": len(schema),
            "data_size_mb": round(data_size / (1024 * 1024)),
            "source_id": source_id,
        }

    def detect_schema_drift(self, path_id: int) -> dict[str, Any]:
        """检测湖仓 Schema 漂移：对比 source_registry，通知 freshness_mon，更新 data_contract。"""
        path_entry = store.find(self.MODULE, path_id)
        if path_entry is None:
            return {"ok": False, "message": "湖仓路径不存在"}

        storage = path_entry.get("存储位置", "")
        recorded_schema = split_fields(path_entry.get("Schema推导", ""))

        # 重新扫描，并按字段列表比较（忽略格式和顺序），避免整串被当成一列。
        new_schema = split_fields(_derive_schema(path_entry.get("文件格式", "parquet")))
        diff = field_diff(new_schema, recorded_schema)
        new_fields = diff["additions"]
        removed_fields = diff["removals"]

        has_drift = diff["has_changes"]

        if has_drift:
            path_entry["Schema推导"] = ", ".join(new_schema)
            path_entry["status"] = "Schema变更"
            path_entry["pending"] = True
            path_entry["最后扫描"] = time.strftime("%Y-%m-%d %H:%M:%S")

            # 通知 freshness_mon
            _notify_schema_drift(path_id, new_fields, removed_fields)

            # 更新数据契约
            _update_contract_on_drift(storage, has_drift)

        return {
            "ok": True,
            "path_id": path_id,
            "has_drift": has_drift,
            "new_fields": new_fields,
            "removed_fields": removed_fields,
            "recorded_count": len(recorded_schema),
            "new_count": len(new_schema),
        }

    def estimate_partition_scan_cost(
        self, path_id: int, date_range: tuple[str, str]
    ) -> dict[str, Any]:
        """估算分区扫描代价并写入 cost_govern。"""
        path_entry = store.find(self.MODULE, path_id)
        if path_entry is None:
            return {"ok": False, "message": "湖仓路径不存在"}

        file_count = len(path_entry.get("文件列表", "").split(","))
        estimated_rows = file_count * random.randint(1000, 100000)
        estimated_io = estimated_rows * 0.002  # MB

        # 写入成本治理
        cost_rows = store.rows("cost_govern")
        cost_id = max((int(r.get("id", 0)) for r in cost_rows), default=0) + 1
        cost = {
            "id": cost_id,
            "策略编号": f"COST-LK{path_id:04d}",
            "适用角色": "lake_reader",
            "查询上限": str(estimated_rows),
            "并发上限": str(file_count),
            "月度配额": str(estimated_rows * 30),
            "告警阈值": str(int(estimated_io * 1.5)),
            "限流动作": "throttle",
            "生效时段": f"{date_range[0]}~{date_range[1]}",
            "status": "已生效" if estimated_io < 5000 else "已告警",
            "pending": estimated_io >= 5000,
            "abnormal": estimated_io >= 10000,
        }
        cost_rows.append(cost)

        return {
            "ok": True,
            "estimated_rows": estimated_rows,
            "estimated_io_mb": round(estimated_io, 2),
            "file_count": file_count,
            "cost_entry_id": cost_id,
        }


def list_lake_diff_records(source_id: int | None = None) -> list[dict[str, Any]]:
    """按来源列出湖仓写入的实际字段差异记录，供来源差异清单关联核对。

    source_id 为空时返回全部来源的实际差异记录。
    """
    prefix = f"source_registry/{source_id}" if source_id not in (None, "") else None
    records = []
    for row in store.rows("data_lake_read"):
        if row.get("文件格式") != "schema_change":
            continue
        if prefix is not None and str(row.get("存储位置")) != prefix:
            continue
        records.append({
            "id": row.get("id"),
            "路径编号": row.get("路径编号"),
            "存储位置": row.get("存储位置"),
            "source_id": row.get("source_id")
            or _source_id_from_storage(row.get("存储位置")),
            "来源编号": row.get("来源编号"),
            "来源差异编号": row.get("来源差异编号"),
            "扫描批次": row.get("扫描批次"),
            "差异指纹": row.get("差异指纹"),
            "实际字段差异": row.get("实际字段差异"),
            "Schema推导": row.get("Schema推导"),
            "差异依据": row.get("差异依据"),
            "最后扫描": row.get("最后扫描"),
            "status": row.get("status"),
            "pending": row.get("pending"),
            "abnormal": row.get("abnormal"),
        })
    return records


def _source_id_from_storage(storage: Any) -> int | None:
    match = re.fullmatch(r"source_registry/(\d+)", str(storage or ""))
    return int(match.group(1)) if match else None


def _as_list(raw: Any) -> list[str]:
    """差异字段既可能是列表也可能已经是逗号串，统一摊平后去空。"""
    if isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        items = str(raw if raw is not None else "").replace("，", ",").split(",")
    return [str(item).strip() for item in items if str(item).strip()]


def _ensure_diff_fields(row: dict[str, Any], diff_row: dict[str, Any]) -> None:
    """已登记的湖仓记录补齐来源侧引用，保持两侧口径一致。"""
    for field in ("来源差异编号", "来源差异id", "扫描批次", "差异指纹", "差异依据"):
        if diff_row.get(field) not in (None, "") and row.get(field) in (None, ""):
            row[field] = diff_row.get(field)
    if not row.get("实际字段差异"):
        row["实际字段差异"] = stable_json({
            "新增字段": _as_list(diff_row.get("新增字段")),
            "移除字段": _as_list(diff_row.get("移除字段")),
            "变化摘要": diff_row.get("变化摘要"),
        })


def _write_source_diff_audit(record: dict[str, Any]) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": (
            f"LAKE_SCHEMA_DIFF source={record.get('source_id')} "
            f"batch={record.get('扫描批次')} diff={record.get('来源差异编号')}"
        ),
        "执行耗时": "80ms",
        "扫描行数": record.get("数据量", "0"),
        "返回行数": "1",
        "数据来源": "data_lake_read",
        "操作用户": "lake_scanner",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": bool(record.get("abnormal")),
    }
    rows.append(audit)


def _derive_schema(fmt: str) -> list[str]:
    schemas = {
        "parquet": ["uid", "action", "action_time", "platform", "version", "event_type", "properties"],
        "orc": ["id", "user_id", "order_amount", "order_time", "region", "category"],
        "json": ["event_id", "event_type", "event_time", "payload", "source"],
        "csv": ["col_1", "col_2", "col_3", "col_4", "col_5"],
        "avro": ["user_id", "session_id", "page", "duration", "actions", "device"],
    }
    return schemas.get(fmt, ["field_1", "field_2", "field_3"])


def _register_as_source(storage: str, fmt: str, schema: list[str]) -> int:
    rows = store.rows("source_registry")
    source_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    source = {
        "id": source_id,
        "源编号": f"SOUR-L{source_id:04d}",
        "源类型": fmt,
        "连接地址": storage,
        "驱动配置": f"lakehouse/{fmt}",
        "数据库名": storage.rsplit("/", 1)[-1],
        "认证方式": "lakehouse_iam",
        "接入状态": "已接入",
        "最近同步": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "已注册",
        "pending": True,
        "abnormal": False,
    }
    rows.append(source)
    return source_id


def _create_semantic_model_for_lake(source_id: int, storage: str, schema: list[str]) -> None:
    rows = store.rows("semantic_model")
    model_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    model = {
        "id": model_id,
        "模型编号": f"SEMA-L{source_id:04d}",
        "模型名称": f"湖仓自动发现_{storage.rsplit('/',1)[-1]}",
        "源表映射": f"data_lake_read:{source_id}",
        "字段别名": ", ".join(schema),
        "度量定义": "COUNT(*), SUM(metrics)",
        "维度定义": ", ".join(schema[:3]),
        "计算口径": "湖仓自动推导",
        "版本序号": "v1.0-lake",
        "status": "已投产",
        "pending": True,
        "abnormal": False,
    }
    rows.append(model)


def _audit_lake_scan(path_id: int, storage: str, file_count: int) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"LAKE_SCAN path_id={path_id} storage={storage}",
        "执行耗时": "500ms",
        "扫描行数": str(file_count),
        "返回行数": str(file_count),
        "数据来源": "data_lake_read",
        "操作用户": "lake_scanner",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)


def _notify_schema_drift(path_id: int, new_fields: list[str], removed_fields: list[str]) -> None:
    rows = store.rows("freshness_mon")
    mon_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    mon = {
        "id": mon_id,
        "规则编号": f"FRES-D{path_id:04d}",
        "目标数据集": f"data_lake_read:{path_id}",
        "SLA秒数": "3600",
        "检测频率": "on_change",
        "延迟阈值": "Schema漂移检测",
        "告警级别": "WARNING" if len(new_fields) > 2 else "INFO",
        "触发次数": "1",
        "最后检查": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "已告警",
        "pending": True,
        "abnormal": len(removed_fields) > 0,
    }
    rows.append(mon)


def _update_contract_on_drift(storage: str, has_drift: bool) -> None:
    contracts = store.rows("data_contract")
    for c in contracts:
        if storage in str(c.get("数据集", "")):
            if has_drift and c.get("status") == "已承诺":
                c["status"] = "已违约"