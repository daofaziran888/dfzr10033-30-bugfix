"""Schema 字段差异公共逻辑。

这里只放跨模块复用的纯函数：字段串分列、格式归一、无序比较，以及来源标识的
精确匹配。来源探测（source_registry）、语义冲突（semantic_model）和湖仓
差异登记（data_lake_read）共用同一套口径，避免各自 split(",") 造成口径漂移。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from typing import Any, Iterable

# 字段列表可能来自表单、模型映射或湖仓 Schema，分隔符和包装字符都要兼容。
_SEPARATOR_RE = re.compile(r"[,，;；\n\r]+")
_WRAP_CHARS = " \t\r\n`'\"[](){}【】（）"
_VERSION_NUM_RE = re.compile(r"\d+")
_MODEL_REF_RE = re.compile(r"([A-Za-z_][\w]*)\s*:\s*(\d+)(?![0-9])")
_STATUS_RANK = {"已投产": 3, "校验中": 2, "草稿": 1, "已废弃": 0}


def normalize_field_name(value: Any) -> str:
    """只去掉展示格式，不改变字段本身的语义。"""
    text = str(value if value is not None else "")
    text = text.strip()
    # 去掉列表/引号/反引号等包装，再压缩内部空白。
    text = text.strip(_WRAP_CHARS)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def field_key(value: Any) -> str:
    """比较用键：大小写、前后空白和包装格式都不参与差异判断。"""
    return normalize_field_name(value).casefold()


def split_fields(raw: Any) -> list[str]:
    """把任意形态的字段串解析为字段列表，保留首次出现的展示名并去重。

    支持 list/tuple/set、JSON 数组、中英文逗号/分号/换行分隔的字符串。
    解析结果保持原顺序；差异判断再单独忽略顺序。
    """
    if raw is None:
        return []
    if isinstance(raw, dict):
        items: Iterable[Any] = list(raw.keys())
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        text = str(raw).strip()
        if not text:
            return []
        parsed: Any = None
        if (text.startswith("[") and text.endswith("]")) or (
            text.startswith("{") and text.endswith("}")
        ):
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError):
                parsed = None
        if isinstance(parsed, dict):
            items = list(parsed.keys())
        elif isinstance(parsed, list):
            items = parsed
        else:
            items = [text]

    fields: list[str] = []
    seen: set[str] = set()
    for item in items:
        # list 中仍可能混入 "a,b" 这种整串，继续按分隔符摊平。
        for piece in _SEPARATOR_RE.split(str(item)):
            name = normalize_field_name(piece)
            if not name:
                continue
            key = name.casefold()
            if key in seen:
                continue
            seen.add(key)
            fields.append(name)
    return fields


def field_diff(current: Any, recorded: Any) -> dict[str, Any]:
    """分列比较当前探测字段与已记录字段。

    返回的 added/removed 使用各自一侧的原始展示名。顺序变化不算差异；
    字段集合完全一致时 reordered=True，但 has_changes 仍为 False。
    """
    current_fields = split_fields(current)
    recorded_fields = split_fields(recorded)

    current_keys = {field_key(f) for f in current_fields}
    recorded_keys = {field_key(f) for f in recorded_fields}

    added = [f for f in current_fields if field_key(f) not in recorded_keys]
    removed = [f for f in recorded_fields if field_key(f) not in current_keys]
    # 顺序变化只有在本来就没有增删时才算「无意义变化」，用 key 顺序判断，
    # 避免仅大小写/空白差异被重复算一次重排。
    reordered = (
        not added
        and not removed
        and [field_key(f) for f in current_fields] != [field_key(f) for f in recorded_fields]
    )

    return {
        "additions": added,
        "removals": removed,
        "new_fields": added,
        "removed_fields": removed,
        "added": added,
        "removed": removed,
        "current_fields": current_fields,
        "recorded_fields": recorded_fields,
        "reordered": reordered,
        "has_changes": bool(added or removed),
    }


def model_version_key(model: dict[str, Any]) -> tuple[Any, ...]:
    """模型版本排序键：优先数字版本，再以状态和 id 兜底。"""
    version_text = str(model.get("版本序号") or model.get("version") or "")
    version = tuple(int(part) for part in _VERSION_NUM_RE.findall(version_text))
    status_rank = _STATUS_RANK.get(str(model.get("status", "")), 0)
    try:
        model_id = int(model.get("id") or 0)
    except (TypeError, ValueError):
        model_id = 0
    return version, status_rank, model_id


def model_fingerprint(model: dict[str, Any]) -> str:
    """一次模型映射快照的指纹，用于证明差异只用了这一份定义。"""
    payload = {
        "id": model.get("id"),
        "模型编号": model.get("模型编号"),
        "源表映射": model.get("源表映射"),
        "版本序号": model.get("版本序号"),
        "状态": model.get("status"),
        "字段别名": model.get("字段别名"),
        "字段列表": split_fields(model.get("字段别名")),
    }
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def source_ref_matches(
    ref: Any,
    module: str,
    source_id: int | str,
    source_no: str | None = None,
) -> bool:
    """按完整来源标识匹配模型映射，避免 source_registry:1 命中 :10/:12。"""
    text = str(ref if ref is not None else "")
    if not text:
        return False

    # source_registry:1、source_registry:1/table 都能精确取到数字 id；
    # 后面的 (?![\w]) 保证 :1 不会命中 :10。
    for module_part, id_part in _MODEL_REF_RE.findall(text):
        if module_part != module:
            continue
        try:
            if int(id_part) == int(source_id):
                return True
        except (TypeError, ValueError):
            continue

    if source_no:
        # 源编号可能以字符串形式出现，需要完整的词边界，避免 SOUR-0001 命中
        # SOUR-00010；同时兼容源表映射中带库/表后缀的写法。
        pattern = r"(?<![A-Za-z0-9_-]){}(?![A-Za-z0-9_-])".format(
            re.escape(str(source_no))
        )
        if re.search(pattern, text):
            return True
    return False


def model_matches_source(
    model: dict[str, Any],
    source_id: int | str,
    source_no: str | None = None,
    module: str = "source_registry",
) -> bool:
    return source_ref_matches(model.get("源表映射", ""), module, source_id, source_no)


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


# ---------- 扫描互斥与批次号 ----------
#
# 存储层是进程内内存表，没有数据库行锁。并发扫描要保证「同一来源只确认一次
# 真实变化」，必须把「读模型映射 → 解析探测字段 → 比较 → 写差异」整段串行化。
# 用可重入锁 + 每次扫描递增的批次序号来固定本轮依据。

_scan_lock = threading.RLock()
_batch_counter = 0


def scan_guard() -> threading.RLock:
    return _scan_lock


def next_scan_batch(source_id: int | str) -> str:
    global _batch_counter
    with _scan_lock:
        _batch_counter += 1
        stamp = time.strftime("%Y%m%d%H%M%S")
        return f"SCAN-{stamp}-{int(source_id):04d}-{_batch_counter:04d}"
