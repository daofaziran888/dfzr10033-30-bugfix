"""数据源注册领域服务：统一接入异构数据源，触发Schema自动发现与连接探测。"""
from __future__ import annotations

import copy
import hashlib
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.schema_diff import (
    field_diff,
    field_key,
    model_fingerprint,
    model_matches_source,
    model_version_key,
    next_scan_batch,
    scan_guard,
    split_fields,
    stable_json,
)
from app.store import store

SOURCE_SCHEMA_DIFF_TABLE = "source_schema_diff"


class SourceRegistryService(BaseModuleService):
    MODULE = "source_registry"
    ENTITY = "注册数据源"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源编号", "源类型", "连接地址", "驱动配置", "数据库名", "认证方式", "接入状态", "最近同步"]
    REQUIRED_FIELDS = ["源编号", "源类型", "连接地址"]
    STATUS_ORDER = ["待验证", "已注册", "连接异常", "已停用"]
    ACTION_RULES = {"验证连接": "已注册", "标记注册": "连接异常", "停用来源": "已停用"}
    NEGATIVE_ACTIONS = ["停用来源"]

    def register_source(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """注册数据源并自动触发：Schema发现→semantic_model、审计日志、数据契约草案。"""
        entry, missing = self.create_entry(values)
        if missing:
            return None, f"缺少必填字段：{'、'.join(missing)}"
        if entry is None:
            return None, "创建数据源失败"

        # 触发Schema发现 —— 模拟从数据源推导表结构
        schema = _discover_schema(values.get("源类型", "unknown"))
        if schema:
            _write_semantic_model(entry["id"], values.get("源编号", ""), schema)

        # 写入审计日志
        _write_audit(entry["id"], values.get("源编号", ""), "register")

        # 建立数据契约草案
        _create_contract_draft(entry["id"], values.get("源编号", ""))

        # 更新最近同步时间和状态
        entry["最近同步"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["接入状态"] = "已接入"
        entry["status"] = "已注册"
        entry["pending"] = True

        return entry, f"数据源{entry['源编号']}已注册并完成Schema发现"

    def detect_schema_changes(
        self, source_id: int, probe_fields: Any = None
    ) -> dict[str, Any]:
        """检测数据源Schema变化并与已记录的语义模型做差异比较。

        取数链路：
        1. 重新探测来源得到「当前字段」（一行一列，不是整串）；
        2. 取该来源既有语义模型的「字段别名」，按分隔符分成字段列表；
        3. 两边分列、归一格式后比较，忽略无意义的顺序变化；
        4. 只为真实增删生成差异，同一来源同一批依据只确认一次，并把确认结果
           同时写入来源差异清单（source_schema_diff）和湖仓读取的差异记录。
        """
        # 整轮扫描串行化：避免两个扫描线程各读一半模型映射、各写一条差异。
        with scan_guard():
            entry = store.find(self.MODULE, source_id)
            if entry is None:
                return {"ok": False, "message": "数据源不存在"}

            source_no = str(entry.get("源编号", ""))
            batch = next_scan_batch(source_id)

            # 探测结果与模型映射各只取一次快照，后续比较只使用快照里的数据。
            current_schema = _probe_fields(entry, probe_fields)
            recorded_models = _get_semantic_models_for_source(source_id, source_no)

            # 来源还没有任何模型映射时，本次探测就是基线：登记映射后不报差异，
            # 避免把当前全部字段误判成新增。
            baseline_created = False
            if not recorded_models:
                _write_semantic_model(source_id, source_no, current_schema)
                recorded_models = _get_semantic_models_for_source(source_id, source_no)
                baseline_created = True

            basis = _build_basis(source_id, current_schema, recorded_models, batch, entry)

            diff = field_diff(current_schema, basis["recorded_fields"])
            # 没有可用模型映射时无法确认「相对既有定义」的增删，只建立基线，
            # 不能把当前全部字段当成新增。
            has_baseline = bool(basis["recorded_fields"]) and basis["model_id"] is not None
            has_changes = diff["has_changes"] and has_baseline
            if not has_baseline:
                diff = dict(diff, additions=[], removals=[], new_fields=[], removed_fields=[])

            if has_changes:
                confirmed = _confirm_diff(source_id, source_no, entry, basis, diff, batch)
            else:
                confirmed = None
                _settle_aligned_source(entry)

            result = {
                "ok": True,
                "source_id": source_id,
                "source_no": source_no,
                "source_type": entry.get("源类型"),
                "batch": batch,
                "current_fields": diff["current_fields"],
                "recorded_fields": diff["recorded_fields"],
                "recorded_fields_count": len(diff["recorded_fields"]),
                "new_fields": diff["new_fields"],
                "removed_fields": diff["removed_fields"],
                "reordered_only": diff["reordered"] and has_baseline,
                "has_baseline": has_baseline,
                "baseline_created": baseline_created,
                "has_changes": has_changes,
                "basis": basis["evidence"],
                "diff_record": confirmed,
                "recorded_diff_count": len(_get_source_diffs(source_id, source_no)),
            }

        return result

    def list_schema_diffs(self, source_id: int) -> dict[str, Any]:
        """来源差异清单，附带关联的湖仓实际差异记录。"""
        return list_source_schema_diffs(source_id)

    def set_probe_fields(self, source_id: int, probe_fields: Any) -> tuple[dict[str, Any] | None, str]:
        """记录一次真实探测结果（模拟源端表结构变化），供下一次扫描比较。"""
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return None, "数据源不存在"
        fields = split_fields(probe_fields)
        if not fields:
            return None, "探测字段不能为空"
        entry["探测字段"] = ", ".join(fields)
        return entry, f"数据源{entry.get('源编号', source_id)}探测字段已更新"


# ---------- 跨模块辅助函数 ----------

def _discover_schema(source_type: str) -> list[str]:
    """模拟从不同数据源类型推导Schema字段列表。"""
    schemas = {
        "mysql": ["id", "user_id", "order_amount", "order_time", "region", "category", "status"],
        "postgresql": ["id", "event_id", "event_type", "event_time", "user_id", "properties"],
        "clickhouse": ["date", "dimension_key", "metric_value", "partition_id", "timestamp"],
        "hive": ["dt", "uid", "action", "action_time", "platform", "version", "extra"],
        "kafka": ["topic", "partition", "offset", "key", "value", "timestamp"],
    }
    return schemas.get(source_type, ["field_1", "field_2", "field_3", "field_4"])


def _write_semantic_model(source_id: int, source_no: str, schema: list[str]) -> None:
    rows = store.rows("semantic_model")
    model_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    model = {
        "id": model_id,
        "模型编号": f"SEMA-S{source_id:04d}",
        "模型名称": f"自动发现_{source_no}",
        "源表映射": f"source_registry:{source_id}",
        "字段别名": ", ".join(schema),
        "度量定义": f"COUNT(*) FROM auto_discovered_{source_id}",
        "维度定义": ", ".join(schema[:3]),
        "计算口径": "自动推导",
        "版本序号": "v1.0-auto",
        "status": "已投产",
        "pending": True,
        "abnormal": False,
    }
    rows.append(model)


def _write_audit(source_id: int, source_no: str, action: str) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"source_registry.{action}:{source_no}",
        "执行耗时": "120ms",
        "扫描行数": str(source_id),
        "返回行数": "1",
        "数据来源": "source_registry",
        "操作用户": "auto_discover",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)


def _create_contract_draft(source_id: int, source_no: str) -> None:
    rows = store.rows("data_contract")
    contract_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    contract = {
        "id": contract_id,
        "契约编号": f"DATA-{uuid.uuid4().hex[:8]}",
        "数据集": f"auto_{source_no}",
        "Schema约束": "自动推导",
        "SLA定义": "每小时更新",
        "更新频率": "3600",
        "负责人": "auto_discover",
        "消费者": "semantic_model",
        "生效时间": time.strftime("%Y-%m-%d"),
        "status": "草稿",
        "pending": True,
        "abnormal": False,
    }
    rows.append(contract)


def _get_semantic_models_for_source(source_id: int, source_no: str | None = None) -> list[dict[str, Any]]:
    """按完整来源标识取模型，排除 source_registry:10 这类相邻编号。

    读取时就深拷贝成快照：别人同时改「字段别名」时，本轮依据仍是同一份定义。
    """
    rows = store.rows("semantic_model")
    return [
        copy.deepcopy(r) for r in rows
        if model_matches_source(r, source_id, source_no, module="source_registry")
    ]


def _probe_fields(entry: dict[str, Any], probe_fields: Any = None) -> list[str]:
    """取当前探测字段。默认按源类型推导，也允许来源上已保存的探测结果覆盖。"""
    if probe_fields is not None:
        return split_fields(probe_fields)
    # 登记时或上一次真实探测可以写入「探测字段」，用于模拟源端表结构调整。
    recorded_probe = entry.get("探测字段")
    if recorded_probe not in (None, ""):
        return split_fields(recorded_probe)
    return split_fields(_discover_schema(entry.get("源类型", "unknown")))


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def _build_basis(
    source_id: int,
    current_fields: list[str],
    models: list[dict[str, Any]],
    batch: str,
    entry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """固定本轮扫描的探测依据与模型映射依据。

    模型只选一个版本作为口径（版本最高、状态最靠前、id 最大），并把当时看到的
    字段别名深拷贝进依据里，后续比较不再回读模型，避免两次读取混用不同定义。
    """
    ordered = sorted(models, key=model_version_key, reverse=True)
    chosen = ordered[0] if ordered else None
    recorded_fields = split_fields(chosen.get("字段别名")) if chosen else []

    probe_evidence = {
        "来源id": source_id,
        "来源编号": entry.get("源编号") if entry else None,
        "源类型": entry.get("源类型") if entry else None,
        "探测字段": list(current_fields),
        "探测字段数": len(current_fields),
        "探测指纹": _fingerprint({"source_id": source_id, "fields": current_fields}),
    }

    if chosen is None:
        mapping_evidence = {
            "模型id": None,
            "模型编号": None,
            "版本序号": None,
            "状态": None,
            "字段别名原文": None,
            "记录字段": [],
            "记录字段数": 0,
            "映射指纹": None,
            "说明": "来源尚无模型映射，按空基线比较",
        }
    else:
        snapshot = copy.deepcopy(chosen)
        mapping_evidence = {
            "模型id": snapshot.get("id"),
            "模型编号": snapshot.get("模型编号"),
            "模型名称": snapshot.get("模型名称"),
            "源表映射": snapshot.get("源表映射"),
            "版本序号": snapshot.get("版本序号"),
            "状态": snapshot.get("status"),
            "字段别名原文": snapshot.get("字段别名"),
            "记录字段": list(recorded_fields),
            "记录字段数": len(recorded_fields),
            "映射指纹": model_fingerprint(snapshot),
        }

    evidence = {
        "探测批次": batch,
        "探测依据": probe_evidence,
        "映射依据": mapping_evidence,
        "同源模型": [
            {
                "模型id": m.get("id"),
                "模型编号": m.get("模型编号"),
                "版本序号": m.get("版本序号"),
                "状态": m.get("status"),
                "映射指纹": model_fingerprint(m),
            }
            for m in ordered
        ],
        "口径": "探测字段列表与单一模型版本的字段列表分列比较；忽略格式与顺序",
    }

    return {
        "chosen_model": copy.deepcopy(chosen) if chosen else None,
        "model_id": chosen.get("id") if chosen else None,
        "mapping_fingerprint": mapping_evidence["映射指纹"],
        "probe_fingerprint": probe_evidence["探测指纹"],
        "recorded_fields": recorded_fields,
        "evidence": evidence,
    }


def _settle_aligned_source(entry: dict[str, Any]) -> None:
    """字段一致时撤掉由探测误判留下的变更标记；其他状态不动。"""
    if entry.get("接入状态") == "Schema变更":
        entry["接入状态"] = "已接入"
        entry["pending"] = False


def _source_change_key(additions: list[str], removals: list[str]) -> str:
    payload = {
        "additions": sorted(field_key(f) for f in additions),
        "removals": sorted(field_key(f) for f in removals),
    }
    return _fingerprint(payload)


def _confirm_diff(
    source_id: int,
    source_no: str,
    entry: dict[str, Any],
    basis: dict[str, Any],
    diff: dict[str, Any],
    batch: str,
) -> dict[str, Any]:
    """确认一次真实变化：同一变化只落一条清单和一条湖仓记录。"""
    change_key = _source_change_key(diff["additions"], diff["removals"])
    entry["接入状态"] = "Schema变更"
    entry["pending"] = True

    existing = _find_confirmed_diff(source_id, change_key, source_no)
    if existing is not None:
        # 已有同口径确认（已发布或历史记录）时复用，不重复生成批次和湖仓记录。
        _link_lake_record_if_missing(source_id, source_no, existing)
        if existing.get("legacy"):
            # 历史湖仓记录没有来源清单条目，补一条清单索引并指回原湖仓记录，
            # 之后的扫描即可直接命中来源清单。
            bridged = _source_diff_record(source_id, source_no, basis, diff, batch, change_key)
            bridged["湖仓记录id"] = existing.get("湖仓记录id") or existing.get("id")
            bridged["湖仓差异状态"] = "引用既有记录"
            store.rows(SOURCE_SCHEMA_DIFF_TABLE).append(bridged)
            return bridged
        reused = dict(existing)
        reused["复用"] = True
        return reused

    record = _source_diff_record(source_id, source_no, basis, diff, batch, change_key)
    store.rows(SOURCE_SCHEMA_DIFF_TABLE).append(record)

    # 由湖仓读取按来源实际字段差异写入实际差异记录，并回写关联关系。
    from app.services.data_lake_read import DataLakeReadService

    lake_record = DataLakeReadService().record_source_field_diff(record, entry)
    if lake_record is not None:
        record["湖仓记录id"] = lake_record.get("id")
        record["湖仓差异状态"] = "已登记"
        record["湖仓写入时间"] = lake_record.get("最后扫描")

    return record


def _source_diff_record(
    source_id: int,
    source_no: str,
    basis: dict[str, Any],
    diff: dict[str, Any],
    batch: str,
    change_key: str,
) -> dict[str, Any]:
    rows = store.rows(SOURCE_SCHEMA_DIFF_TABLE)
    diff_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    evidence = copy.deepcopy(basis["evidence"])
    evidence.pop("探测批次", None)
    evidence["差异指纹"] = change_key

    # 列表页展示用的短字段，完整依据另存为 JSON，便于湖仓详情直接引用。
    additions_text = ",".join(diff["additions"])
    removals_text = ",".join(diff["removals"])
    return {
        "id": diff_id,
        "来源差异编号": f"SRCDIFF-{source_id:04d}-{diff_id:04d}",
        "source_id": source_id,
        "来源编号": source_no,
        "扫描批次": batch,
        "新增字段": additions_text,
        "移除字段": removals_text,
        "新增字段数": len(diff["additions"]),
        "移除字段数": len(diff["removals"]),
        "变化摘要": f"+{additions_text} / -{removals_text}",
        "模型id": basis.get("model_id"),
        "模型版本": evidence["映射依据"].get("版本序号"),
        "探测指纹": basis.get("probe_fingerprint"),
        "映射指纹": basis.get("mapping_fingerprint"),
        "差异指纹": change_key,
        "差异依据": stable_json(evidence),
        "湖仓记录id": None,
        "湖仓差异状态": "待登记",
        "最后确认": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "已确认",
        "pending": True,
        "abnormal": False,
    }


def _normalize_diff_row(
    row: dict[str, Any], source_id: int, source_no: str | None = None
) -> dict[str, Any] | None:
    """把清单记录或旧版湖仓记录统一成「来源差异」视图。"""
    if row.get("来源差异编号"):
        # 新清单记录按 source_id 归属；既有记录若只有源编号则按完整编号兜底。
        if row.get("source_id") not in (None, ""):
            if int(row.get("source_id")) != source_id:
                return None
        elif not (source_no and str(row.get("来源编号")) == str(source_no)):
            return None
        return row

    # 旧版湖仓记录：文件格式=schema_change，存储位置=source_registry/{id}，
    # Schema推导 形如 "+a,b / -c"。兼容已有数据，不要求重新扫描。
    if row.get("文件格式") != "schema_change":
        return None
    if str(row.get("存储位置", "")) != f"source_registry/{source_id}":
        return None

    # 旧版格式固定为 "+a,b / -c"，两侧都按字段分隔符摊平，不再把整串当一列。
    summary = str(row.get("Schema推导", ""))
    additions_text, _, removals_text = summary.partition(" / ")
    additions = split_fields(additions_text.removeprefix("+").strip())
    removals = split_fields(
        removals_text.removeprefix("-").strip() if removals_text else ""
    )
    return {
        "id": row.get("id"),
        "来源差异编号": str(row.get("路径编号") or f"DATA-SC{source_id:04d}-LEGACY"),
        "source_id": source_id,
        "来源编号": row.get("来源编号") or _source_no_for(source_id),
        "扫描批次": str(row.get("扫描批次") or f"LEGACY-{row.get('id')}"),
        "新增字段": ",".join(additions),
        "移除字段": ",".join(removals),
        "新增字段数": len(additions),
        "移除字段数": len(removals),
        "变化摘要": summary,
        "差异指纹": _source_change_key(additions, removals),
        "湖仓记录id": row.get("id"),
        "湖仓差异状态": "历史记录",
        "最后确认": row.get("最后扫描"),
        "status": row.get("status", "Schema变更"),
        "pending": bool(row.get("pending")),
        "abnormal": bool(row.get("abnormal")),
        "legacy": True,
    }


def _source_no_for(source_id: int) -> str | None:
    entry = store.find("source_registry", source_id)
    return str(entry.get("源编号")) if entry else None


def _get_source_diffs(source_id: int, source_no: str | None = None) -> list[dict[str, Any]]:
    """来源差异清单：新清单为主，同时并入既有已发布/历史湖仓差异。"""
    diffs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in store.rows(SOURCE_SCHEMA_DIFF_TABLE):
        normalized = _normalize_diff_row(row, source_id, source_no)
        if normalized is None:
            continue
        diffs.append(normalized)
        seen.add(str(normalized.get("差异指纹")))

    for row in store.rows("data_lake_read"):
        normalized = _normalize_diff_row(row, source_id, source_no)
        if normalized is None or str(normalized.get("差异指纹")) in seen:
            continue
        diffs.append(normalized)
        seen.add(str(normalized.get("差异指纹")))

    return diffs


def _find_confirmed_diff(
    source_id: int, change_key: str, source_no: str | None = None
) -> dict[str, Any] | None:
    for diff in _get_source_diffs(source_id, source_no):
        if str(diff.get("差异指纹")) == change_key:
            return diff
    return None


def _link_lake_record_if_missing(source_id: int, source_no: str, diff_row: dict[str, Any]) -> None:
    if diff_row.get("湖仓记录id") or diff_row.get("legacy"):
        return
    from app.services.data_lake_read import DataLakeReadService

    lake_record = DataLakeReadService().record_source_field_diff(diff_row)
    if lake_record is not None:
        diff_row["湖仓记录id"] = lake_record.get("id")
        diff_row["湖仓差异状态"] = "已登记"
        diff_row["湖仓写入时间"] = lake_record.get("最后扫描")


def list_source_schema_diffs(source_id: int) -> dict[str, Any]:
    """读取来源差异清单，并附带关联的湖仓实际差异记录。"""
    entry = store.find("source_registry", source_id)
    if entry is None:
        return {"ok": False, "message": "数据源不存在", "items": [], "total": 0}

    items = _get_source_diffs(source_id, entry.get("源编号"))
    lake_by_id = {int(r.get("id", 0)): r for r in store.rows("data_lake_read")}
    for item in items:
        lake_id = item.get("湖仓记录id")
        lake = lake_by_id.get(int(lake_id)) if lake_id not in (None, "") else None
        item["湖仓记录"] = (
            {
                "id": lake.get("id"),
                "路径编号": lake.get("路径编号"),
                "存储位置": lake.get("存储位置"),
                "扫描批次": lake.get("扫描批次"),
                "实际字段差异": lake.get("实际字段差异"),
                "Schema推导": lake.get("Schema推导"),
                "最后扫描": lake.get("最后扫描"),
                "status": lake.get("status"),
            }
            if lake else None
        )

    return {
        "ok": True,
        "source_id": source_id,
        "source_no": entry.get("源编号"),
        "items": items,
        "total": len(items),
    }