"""语义模型领域服务：管理表映射、字段别名、度量定义与计算口径，作为查询翻译层。"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.schema_diff import model_matches_source, split_fields
from app.store import store


class SemanticModelService(BaseModuleService):
    MODULE = "semantic_model"
    ENTITY = "语义定义"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["模型编号", "模型名称", "源表映射", "字段别名", "度量定义", "维度定义", "计算口径", "版本序号"]
    REQUIRED_FIELDS = ["模型编号", "模型名称", "源表映射"]
    STATUS_ORDER = ["草稿", "校验中", "已投产", "已废弃"]
    ACTION_RULES = {"提交校验": "校验中", "投产模型": "已投产", "废弃模型": "已废弃"}
    NEGATIVE_ACTIONS = []

    def validate_and_promote(self, model_id: int) -> dict[str, Any]:
        """校验语义模型并投产：验证源表映射、字段别名与度量定义。

        跨模块数据流：
        - 读取 source_registry 校验源表是否存在
        - 读取 query_rewrite 获取可用的改写规则
        - 写入数据契约作为Schema承诺
        - 写入审计日志
        """
        model = store.find(self.MODULE, model_id)
        if model is None:
            return {"ok": False, "message": "语义模型不存在"}

        source_ref = model.get("源表映射", "")
        validation_issues = []

        # 校验源表是否存在
        if "source_registry" in source_ref:
            sources = store.rows("source_registry")
            related = [s for s in sources if str(s.get("id")) in source_ref]
            if not related:
                validation_issues.append("关联的数据源不存在或已停用")

        # 校验字段别名不为空
        aliases = model.get("字段别名", "")
        if not aliases.strip() or aliases.strip() == ",":
            validation_issues.append("字段别名不能为空")

        # 校验度量定义
        measure = model.get("度量定义", "")
        if not measure.strip():
            validation_issues.append("度量定义不能为空")

        if validation_issues:
            model["status"] = "已废弃"
            model["abnormal"] = True
            return {
                "ok": False,
                "message": f"校验失败: {'; '.join(validation_issues)}",
                "issues": validation_issues,
            }

        # 通过校验，投产模型
        model["status"] = "已投产"
        model["pending"] = False

        # 更新数据契约
        _update_contract_on_promotion(model)

        # 写入审计
        _audit_promotion(model_id, model.get("模型编号", ""))

        return {
            "ok": True,
            "model_id": model_id,
            "status": "已投产",
            "fields_count": len(aliases.split(",")),
        }

    def detect_schema_conflicts(self, source_id: int) -> dict[str, Any]:
        """检测数据源Schema变化与已投产语义模型之间的冲突。

        跨模块数据流：
        - 读取 source_registry 的Schema记录
        - 读取 data_lake_read 的Schema变更记录
        - 比较语义模型字段定义，标记冲突
        """
        models = store.rows(self.MODULE)
        conflicts = []

        for model in models:
            if model.get("status") != "已投产":
                continue
            # 按完整来源标识匹配，避免 source_registry:1 命中 :10。
            if not model_matches_source(model, source_id, module="source_registry"):
                continue

            current_fields = split_fields(model.get("字段别名"))

            # 检测 data_lake_read 中该来源的Schema变更（按完整来源标识匹配）
            lake_rows = store.rows("data_lake_read")
            for lake in lake_rows:
                schema_changes = lake.get("Schema推导", "")
                if (
                    lake.get("文件格式") == "schema_change"
                    and str(lake.get("存储位置", "")) == f"source_registry/{source_id}"
                ):
                    conflicts.append({
                        "model_id": model["id"],
                        "model_name": model.get("模型名称"),
                        "source_id": source_id,
                        "schema_change": schema_changes,
                        "current_fields": list(current_fields),
                    })

        return {
            "ok": True,
            "source_id": source_id,
            "conflict_count": len(conflicts),
            "conflicts": conflicts,
        }


def _update_contract_on_promotion(model: dict[str, Any]) -> None:
    contracts = store.rows("data_contract")
    for c in contracts:
        if c.get("status") == "草稿" and model.get("模型名称", "") in str(c.get("数据集", "")):
            c["Schema约束"] = model.get("字段别名", "")
            c["status"] = "已承诺"


def _audit_promotion(model_id: int, model_no: str) -> None:
    rows = store.rows("query_audit")
    audit_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    audit = {
        "id": audit_id,
        "记录编号": f"AUDIT-{uuid.uuid4().hex[:8]}",
        "查询语句": f"SEMANTIC_PROMOTE model={model_no}",
        "执行耗时": "200ms",
        "扫描行数": "1",
        "返回行数": "1",
        "数据来源": "semantic_model",
        "操作用户": "validator",
        "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "正常",
        "pending": False,
        "abnormal": False,
    }
    rows.append(audit)