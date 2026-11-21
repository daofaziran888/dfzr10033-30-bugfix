"""业务路由汇总。"""
from __future__ import annotations
from types import SimpleNamespace

from app.routers import resource

# 固定路径（/api/health、/api/overview、/api/pipelines/...）必须先于动态资源路由
# 注册，否则 /{module}/{entry_id} 会把 pipelines 当成模块名。
ROUTERS = [SimpleNamespace(router=resource.pipeline_router), resource]
