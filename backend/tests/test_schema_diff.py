"""来源探测字段差异：分列比较、来源标识、幂等确认与湖仓回写。"""
import json
import threading

from app.services.data_lake_read import DataLakeReadService, list_lake_diff_records
from app.services.schema_diff import field_diff, model_matches_source, split_fields
from app.services.semantic_model import SemanticModelService
from app.services.source_registry import SourceRegistryService
from app.store import store


def _register(source: SourceRegistryService, no: str, source_type: str = "mysql") -> int:
    entry, message = source.register_source({
        "源编号": no, "源类型": source_type, "连接地址": f"db://{no.lower()}"})
    assert entry is not None, message
    return entry["id"]


class TestFieldDiff:
    def test_comma_string_is_not_a_single_column(self):
        diff = field_diff(["id", "name"], "id,name")
        assert diff["additions"] == [] and diff["removals"] == []
        assert diff["has_changes"] is False

    def test_format_and_order_are_ignored(self):
        assert field_diff([" B ", "a"], "a，b")["has_changes"] is False
        assert field_diff(["a", "b"], "a,b")["reordered"] is False
        assert field_diff(["b", "a"], "a,b")["reordered"] is True
        assert field_diff(["b", "a"], "a,b")["has_changes"] is False

    def test_real_removal_reports_only_that_column(self):
        diff = field_diff(["a", "b"], "a,b,c")
        assert diff["removals"] == ["c"]
        assert diff["additions"] == []

    def test_split_fields_accepts_json_and_cjk_separators(self):
        assert split_fields('["a", "b"]') == ["a", "b"]
        assert split_fields("a；b") == ["a", "b"]

    def test_full_source_id_matching(self):
        assert model_matches_source({"源表映射": "source_registry:1"}, 1)
        assert not model_matches_source({"源表映射": "source_registry:10"}, 1)
        assert not model_matches_source({"源表映射": "source_registry:12"}, 1)
        assert model_matches_source(
            {"源表映射": "source_registry:SOUR-0001"}, 99, "SOUR-0001")
        assert not model_matches_source(
            {"源表映射": "source_registry:SOUR-00010"}, 99, "SOUR-0001")


class TestDetectSchemaChanges:
    def test_register_then_scan_has_no_changes(self):
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-A")

        result = source.detect_schema_changes(source_id)

        assert result["ok"] and result["has_changes"] is False
        assert result["new_fields"] == [] and result["removed_fields"] == []
        assert result["recorded_diff_count"] == 0
        assert store.find("source_registry", source_id)["接入状态"] == "已接入"

    def test_removed_column_confirmed_once_and_written_to_lake(self):
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-B")

        first = source.detect_schema_changes(source_id)
        source.set_probe_fields(source_id, first["current_fields"][:-1])
        second = source.detect_schema_changes(source_id)

        assert second["has_changes"] is True
        assert second["removed_fields"] == ["status"]
        assert second["new_fields"] == []

        lake_records = list_lake_diff_records(source_id)
        assert len(lake_records) == 1
        record = lake_records[0]
        assert record["扫描批次"] == second["batch"]
        actual = json.loads(record["实际字段差异"])
        assert actual["移除字段"] == ["status"] and actual["新增字段"] == []

        # 再扫一次同一变化：不新增批次、不新增湖仓记录。
        third = source.detect_schema_changes(source_id)
        assert third["has_changes"] is True
        assert len(list_lake_diff_records(source_id)) == 1
        assert source.list_schema_diffs(source_id)["total"] == 1

    def test_each_diff_carries_probe_and_mapping_evidence(self):
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-C")
        first = source.detect_schema_changes(source_id)
        source.set_probe_fields(source_id, first["current_fields"] + ["extra_col"])
        result = source.detect_schema_changes(source_id)

        evidence = json.loads(result["diff_record"]["差异依据"])
        assert evidence["探测依据"]["探测字段"] == result["current_fields"]
        assert evidence["映射依据"]["模型id"] == result["diff_record"]["模型id"]
        assert evidence["映射依据"]["记录字段"] == first["recorded_fields"]
        assert evidence["映射依据"]["映射指纹"]

        # 清单条目与湖仓记录引用同一份依据。
        listed = source.list_schema_diffs(source_id)["items"][0]
        assert listed["湖仓记录"]["扫描批次"] == result["batch"]
        lake = next(r for r in store.rows("data_lake_read") if r["id"] == listed["湖仓记录id"])
        assert lake["差异依据"] == listed["差异依据"]

    def test_newer_model_version_is_the_single_basis(self):
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-D")
        store.rows("semantic_model").append({
            "id": 5000, "模型编号": "SEMA-V2", "模型名称": "升级版",
            "源表映射": f"source_registry:{source_id}",
            "字段别名": "id,user_id,order_amount,order_time,region,category,status,new_col",
            "度量定义": "COUNT(*)", "维度定义": "id", "计算口径": "升级",
            "版本序号": "v2.0", "status": "已投产", "pending": False, "abnormal": False})

        result = source.detect_schema_changes(source_id)

        # 以最高版本为口径：v2 比 v1 多 new_col，来源探测少一个字段。
        assert result["basis"]["映射依据"]["版本序号"] == "v2.0"
        assert "new_col" in result["removed_fields"]
        assert result["diff_record"]["模型版本"] == "v2.0"

    def test_adjacent_source_ids_are_not_mixed(self):
        source = SourceRegistryService()
        first = _register(source, "TEST-SCHEMA-10", "hive")
        second = _register(source, "TEST-SCHEMA-12", "hive")
        store.rows("semantic_model").append({
            "id": 5001, "模型编号": "SEMA-ADJ", "模型名称": "相邻编号模型",
            "源表映射": f"source_registry:{first}",
            "字段别名": "dt,uid,action", "度量定义": "COUNT(*)", "维度定义": "dt",
            "计算口径": "手工", "版本序号": "v1.5", "status": "已投产",
            "pending": False, "abnormal": False})

        first_result = source.detect_schema_changes(first)
        second_result = source.detect_schema_changes(second)

        assert first_result["basis"]["映射依据"]["模型id"] == 5001
        assert second_result["basis"]["映射依据"]["模型id"] != 5001
        assert second_result["has_changes"] is False

    def test_semantic_conflicts_ignore_adjacent_source(self):
        source = SourceRegistryService()
        semantic = SemanticModelService()
        source_id = _register(source, "TEST-SCHEMA-E")
        other_id = _register(source, "TEST-SCHEMA-E2")
        store.rows("semantic_model").append({
            "id": 5002, "模型编号": "SEMA-OTHER", "模型名称": "别的来源",
            "源表映射": f"source_registry:{other_id}",
            "字段别名": "x,y", "度量定义": "COUNT(*)", "维度定义": "x",
            "计算口径": "手工", "版本序号": "v1.0", "status": "已投产",
            "pending": False, "abnormal": False})
        # 让来源先产生一条真实差异，冲突检测才有可关联的湖仓记录。
        probe = source.detect_schema_changes(source_id)["current_fields"]
        source.set_probe_fields(source_id, probe[:-1])
        assert source.detect_schema_changes(source_id)["has_changes"] is True

        conflicts = semantic.detect_schema_conflicts(source_id)["conflicts"]
        assert conflicts
        assert all(c["model_id"] != 5002 for c in conflicts)

    def test_concurrent_scans_confirm_single_change(self):
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-F")
        first = source.detect_schema_changes(source_id)
        source.set_probe_fields(source_id, first["current_fields"][:-1])

        results = []
        errors = []

        def scan():
            try:
                results.append(source.detect_schema_changes(source_id))
            except Exception as exc:  # pragma: no cover - 失败时便于定位
                errors.append(exc)

        threads = [threading.Thread(target=scan) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors
        assert len(results) == 8
        assert all(result["has_changes"] for result in results)
        assert len(list_lake_diff_records(source_id)) == 1
        assert source.list_schema_diffs(source_id)["total"] == 1

    def test_legacy_published_diff_is_reused(self):
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-G")
        probe = source.detect_schema_changes(source_id)["current_fields"]
        # 既有已发布差异：旧格式湖仓记录，没有来源清单条目。
        store.rows("data_lake_read").append({
            "id": 9001, "路径编号": "DATA-SC-LEGACY",
            "存储位置": f"source_registry/{source_id}",
            "文件格式": "schema_change", "分区模式": "N/A",
            "Schema推导": f"+ / -{probe[-1]}", "文件列表": "",
            "最后扫描": "2026-11-01 00:00:00", "数据量": "1",
            "status": "Schema变更", "pending": True, "abnormal": False})

        listed = source.list_schema_diffs(source_id)
        assert listed["total"] == 1
        assert listed["items"][0]["移除字段"] == probe[-1]

        # 来源这时真实少了一列，与新清单里既有差异是同一口径。
        source.set_probe_fields(source_id, probe[:-1])
        result = source.detect_schema_changes(source_id)
        assert result["has_changes"] is True
        # 不重复登记实际差异记录，新清单条目指向既有湖仓记录。
        assert len(list_lake_diff_records(source_id)) == 1
        assert result["diff_record"]["湖仓记录id"] == 9001

    def test_aligned_source_clears_false_change_mark(self):
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-H")
        entry = store.find("source_registry", source_id)
        entry["接入状态"] = "Schema变更"

        result = source.detect_schema_changes(source_id)

        assert result["has_changes"] is False
        assert entry["接入状态"] == "已接入"


class TestLakeReadIntegration:
    def test_lake_service_writes_actual_diff_record(self):
        source = SourceRegistryService()
        lake = DataLakeReadService()
        source_id = _register(source, "TEST-SCHEMA-I")
        first = source.detect_schema_changes(source_id)
        source.set_probe_fields(source_id, first["current_fields"][:-1])
        result = source.detect_schema_changes(source_id)

        record = lake.record_source_field_diff(result["diff_record"])
        assert record["来源差异编号"] == result["diff_record"]["来源差异编号"]
        assert record["扫描批次"] == result["batch"]
        assert len(lake.list_lake_diff_records(source_id)) == 1

    def test_api_scan_and_query_returns_only_removed_column(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        source = SourceRegistryService()
        source_id = _register(source, "TEST-SCHEMA-API")

        initial = client.post("/api/pipelines/source-schema-scan",
                              params={"source_id": source_id})
        assert initial.status_code == 200
        assert initial.json()["schema_changes"]["has_changes"] is False

        current = source.detect_schema_changes(source_id)["current_fields"]
        removed = current[-1]
        scan = client.post("/api/pipelines/source-schema-scan",
                           params={"source_id": source_id,
                                   "probe_fields": ",".join(current[:-1])})
        assert scan.status_code == 200
        body = scan.json()
        assert body["schema_changes"]["removed_fields"] == [removed]

        diffs = client.get("/api/pipelines/source-schema-diffs",
                           params={"source_id": source_id}).json()
        assert diffs["total"] == 1
        item = diffs["items"][0]
        assert item["移除字段"] == removed
        assert item["新增字段"] == ""
        assert item["湖仓记录"]["扫描批次"] == body["schema_changes"]["batch"]
        lake = json.loads(item["湖仓记录"]["实际字段差异"])
        assert lake["移除字段"] == [removed] and lake["新增字段"] == []
