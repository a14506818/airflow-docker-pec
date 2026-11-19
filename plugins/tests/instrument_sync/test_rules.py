import pytest

from src.instrument_sync.config import FIELD_MAP, WRITABLE_COLUMNS
from src.instrument_sync.rules import (
    MemberIndex, SapValidationError, alpha_out, clean_sap_account, compute_sync,
    normalize_sap_rows, validate_sap_rows,
)


# 測試資料 ------------------------------------------------------------------------------------------
def sap_row(anln1="161670000530", **kw):
    row = {
        "ANLN1": anln1, "TXT50": "精密天平", "TXA50": "MR3002", "ANLKL": "00161670",
        "TXK50": "儀器設備", "STORT": "C004", "STORT_TXT": "18樓實驗室", "KOSTL": "1000316200",
        "KTEXT": "毒理藥動部", "KFZKZ": "91240401", "ORD41": "1",
    }
    row.update(kw)
    return normalize_sap_rows([row])[0]


def master_row(no=1, asset="161670000530", **kw):
    row = {
        "Instrument_No": no, "Asset_No": asset,
        "Instrument_Name": "精密天平", "Instrument_Spec": "MR3002", "Asset_Type": "161670",
        "Asset_Type_Name": "儀器設備", "Location_Code": "C004", "Location_Name": "18樓實驗室",
        "Cost_Center_Code": "1000316200", "Cost_Center_Name": "毒理藥動部",
        "Keeper": "dy_lee", "Keeper_Name": "李丹洋 DY Lee", "Keeper_SAP_Account": "91240401",
        "Sub_Keeper": None, "Sub_Keeper_Name": None, "Sub_Keeper_SAP_Account": None,
        "Additional_Notify_Keeper": None, "Additional_Notify_Keeper_Name": None,
        "Additional_Notify_Keeper_SAP_Account": None,
    }
    row.update(kw)
    return row


def member(account, name, sap, disabled=False):
    return {"Account": account, "DisplayName": name, "SAP_Account": sap, "Disabled": disabled}


MEMBERS = [
    member("dy_lee", "李丹洋 DY Lee", "91240401"),
    member("amy_wang", "王小美 Amy Wang", "91000001"),
    member("bob_chen", "陳大寶 Bob Chen", "91000002"),
]


def run(master, sap, members=MEMBERS):
    return compute_sync(master, sap, members)


# F1/F2 正規化與檢核 ---------------------------------------------------------------------------------
def test_alpha_strips_leading_zeros():
    assert alpha_out("00161670") == "161670"
    assert alpha_out("0000") == "0"
    assert alpha_out("") == ""
    assert sap_row()["ANLKL"] == "161670"


def test_normalize_strips_whitespace_and_none():
    row = normalize_sap_rows([{"ANLN1": " 161670000530 ", "TXT50": None}])[0]
    assert row["ANLN1"] == "161670000530"
    assert row["TXT50"] == ""
    assert "MENGE" not in row


def test_validate_rejects_empty_result():
    with pytest.raises(SapValidationError, match="0 筆"):
        validate_sap_rows([])


def test_validate_rejects_empty_asset_no():
    with pytest.raises(SapValidationError, match="為空"):
        validate_sap_rows([sap_row(), sap_row(anln1="")])


def test_validate_rejects_duplicate_asset_no():
    with pytest.raises(SapValidationError, match="重複"):
        validate_sap_rows([sap_row(), sap_row()])


def test_validate_passes_normal_rows():
    validate_sap_rows([sap_row(), sap_row(anln1="161670000531")])


# F3 資產欄位 ---------------------------------------------------------------------------------------
def test_no_change_produces_no_update_but_still_synced():
    r = run([master_row()], [sap_row()])
    assert r["updates"] == {}
    assert r["changes"] == []
    assert r["synced"] == [1]


def test_changed_sap_field_is_updated():
    r = run([master_row()], [sap_row(STORT="C005", STORT_TXT="19樓實驗室")])
    assert r["updates"] == {1: {"Location_Code": "C005", "Location_Name": "19樓實驗室"}}
    fields = {c["Field"]: (c["Old"], c["New"]) for c in r["changes"]}
    assert fields["Location_Code"] == ("C004", "C005")
    assert r["stats"]["field_counts"] == {"Location_Code": 1, "Location_Name": 1}


def test_empty_master_field_is_filled():
    r = run([master_row(Instrument_Spec=None, Asset_Type=None)], [sap_row()])
    assert r["updates"][1] == {"Instrument_Spec": "MR3002", "Asset_Type": "161670"}


def test_empty_sap_value_does_not_overwrite():
    r = run([master_row()], [sap_row(TXA50="", KTEXT="  ")])
    assert r["updates"] == {}


def test_whitespace_only_difference_is_not_a_change():
    r = run([master_row(Location_Name=" 18樓實驗室 ")], [sap_row()])
    assert r["updates"] == {}


def test_only_whitelisted_columns_are_ever_written():
    bpm_fields = {"is_GMP": "Y", "Cal_Cycle_Days": 365, "Last_Cal_Date": "2026/01/01",
                  "is_Active": "N", "Acquisition_Date": "2024-09-11 00:00:00",
                  "Contract_Expiry_Date": "2027/01/01", "Next_Due_Date": "2027/01/01"}
    sap = sap_row(TXT50="新名稱", STORT="Z999", KFZKZ="91000001")
    r = run([master_row(**bpm_fields)], [sap])
    written = set(r["updates"][1])
    assert written <= WRITABLE_COLUMNS
    assert not written & set(bpm_fields)


def test_field_map_targets_are_whitelisted():
    assert set(FIELD_MAP.values()) <= WRITABLE_COLUMNS


def test_master_asset_missing_in_sap_is_reported_not_changed():
    r = run([master_row(asset="999999999999")], [sap_row()])
    assert r["synced"] == []
    assert [m["Asset_No"] for m in r["sap_missing"]] == ["999999999999"]
    assert 1 not in r["updates"]


def test_sap_assets_not_in_master_are_ignored():
    r = run([master_row()], [sap_row(), sap_row(anln1="604305000379")])
    assert r["synced"] == [1]
    assert r["stats"]["sap_rows"] == 2


def test_sap_row_missing_required_field_is_skipped_and_reported():
    r = run([master_row()], [sap_row(TXT50="", STORT="C005")])
    assert r["synced"] == []
    assert "Location_Code" not in r["updates"].get(1, {})
    assert len(r["sap_skipped"]) == 1
    assert "TXT50" in r["sap_skipped"][0]["Reason"]


def test_instrument_without_asset_no_is_not_asset_synced():
    r = run([master_row(asset=None, Instrument_Spec=None)], [sap_row()])
    assert r["synced"] == []
    assert r["sap_missing"] == []
    assert r["updates"] == {}


# 保管人：SAP 為準 ------------------------------------------------------------------------------------
def test_sap_keeper_change_replaces_keeper_three_fields():
    r = run([master_row()], [sap_row(KFZKZ="91000001")])
    assert r["updates"][1] == {"Keeper": "amy_wang", "Keeper_Name": "王小美 Amy Wang",
                               "Keeper_SAP_Account": "91000001"}
    kc = r["keeper_changes"][0]
    assert (kc["Old_Keeper"], kc["New_Keeper"], kc["SAP_Keeper"]) == ("dy_lee", "amy_wang", "91000001")


def test_sap_keeper_same_account_refreshes_name_only():
    r = run([master_row(Keeper_Name="舊姓名")], [sap_row()])
    assert r["updates"][1] == {"Keeper_Name": "李丹洋 DY Lee"}
    assert r["keeper_changes"] == []


def test_keeper_account_compare_is_case_insensitive():
    r = run([master_row(Keeper="DY_LEE")], [sap_row()])
    assert "Keeper" not in r["updates"].get(1, {})
    assert r["keeper_changes"] == []


def test_sap_keeper_not_found_leaves_keeper_and_reports():
    r = run([master_row()], [sap_row(KFZKZ="99999999")])
    assert r["updates"] == {}
    a = r["keeper_anomalies"][0]
    assert a["Role"] == "保管人" and a["Value"] == "99999999"
    assert "查無" in a["Reason"]


def test_sap_keeper_resigned_leaves_keeper_and_reports():
    members = MEMBERS + [member("gone", "離職者", "91000009", disabled=True)]
    r = run([master_row()], [sap_row(KFZKZ="91000009")], members)
    assert r["updates"] == {}
    assert "離職" in r["keeper_anomalies"][0]["Reason"]


def test_date_resigned_placeholder_does_not_mean_resigned():
    m = dict(member("dy_lee", "李丹洋 DY Lee", "91240401"), DateResigned="1900/01/01")
    r = run([master_row(Keeper_Name="舊姓名")], [sap_row()], [m])
    assert r["keeper_anomalies"] == []
    assert r["updates"][1] == {"Keeper_Name": "李丹洋 DY Lee"}


def test_sap_keeper_ambiguous_leaves_keeper_and_reports():
    members = MEMBERS + [member("amy_wang2", "王小美2", "91000001")]
    r = run([master_row()], [sap_row(KFZKZ="91000001")], members)
    assert r["updates"] == {}
    assert "多個" in r["keeper_anomalies"][0]["Reason"]


def test_sap_keeper_duplicate_with_one_resigned_picks_active():
    members = MEMBERS + [member("amy_old", "王小美(舊)", "91000001", disabled=True)]
    r = run([master_row()], [sap_row(KFZKZ="91000001")], members)
    assert r["updates"][1]["Keeper"] == "amy_wang"


def test_blank_sap_keeper_falls_back_to_bpm_account_refresh():
    r = run([master_row(Keeper_Name="舊姓名", Keeper_SAP_Account=None)], [sap_row(KFZKZ="")])
    assert r["updates"][1] == {"Keeper_Name": "李丹洋 DY Lee", "Keeper_SAP_Account": "91240401"}


def test_instrument_without_asset_refreshes_keeper_by_account():
    r = run([master_row(asset=None, Keeper="bob_chen", Keeper_Name="舊", Keeper_SAP_Account="")], [sap_row()])
    assert r["updates"][1] == {"Keeper_Name": "陳大寶 Bob Chen", "Keeper_SAP_Account": "91000002"}


def test_empty_keeper_without_sap_is_reported():
    r = run([master_row(asset=None, Keeper=None)], [sap_row()])
    assert r["keeper_anomalies"][0]["Reason"] == "未設定保管人"


# 保管人：F4 各組總保管人／額外通知人 ----------------------------------------------------------------------
def test_sub_and_additional_keepers_refreshed_by_account():
    m = master_row(Sub_Keeper="amy_wang", Sub_Keeper_Name="舊", Sub_Keeper_SAP_Account=None,
                   Additional_Notify_Keeper="Bob_Chen", Additional_Notify_Keeper_Name="陳大寶 Bob Chen",
                   Additional_Notify_Keeper_SAP_Account="91000002")
    r = run([m], [sap_row()])
    assert r["updates"][1] == {"Sub_Keeper_Name": "王小美 Amy Wang", "Sub_Keeper_SAP_Account": "91000001"}


def test_sub_keeper_account_is_never_replaced():
    r = run([master_row(Sub_Keeper="amy_wang")], [sap_row(KFZKZ="91000002")])
    assert "Sub_Keeper" not in r["updates"][1]


def test_sub_keeper_not_found_or_resigned_is_reported():
    members = MEMBERS + [member("gone", "離職者", "91000009", disabled=True)]
    m = master_row(Sub_Keeper="nobody", Additional_Notify_Keeper="gone")
    r = run([m], [sap_row()], members)
    reasons = {a["Role"]: a["Reason"] for a in r["keeper_anomalies"]}
    assert reasons["各組總保管人"] == "人事資料查無此帳號"
    assert reasons["額外通知人"] == "已離職／帳號停用"
    assert r["updates"] == {}


def test_empty_sub_keeper_is_not_an_anomaly():
    r = run([master_row()], [sap_row()])
    assert r["keeper_anomalies"] == []


def test_junk_sap_account_is_treated_as_empty():
    for junk in ("null", "NULL", "NA", " ", None):
        assert clean_sap_account(junk) == ""
    members = [member("dy_lee", "李丹洋 DY Lee", "null")]
    r = run([master_row(asset=None)], [sap_row()], members)
    assert "Keeper_SAP_Account" not in r["updates"].get(1, {})


def test_member_index_ignores_junk_sap_accounts():
    idx = MemberIndex([member("a", "A", "null"), member("b", "B", "null")])
    assert idx.lookup_sap_account("null") == (None, "人事資料查無此 SAP 帳號")


# 摘要 ----------------------------------------------------------------------------------------------
def test_stats_summary():
    master = [master_row(1), master_row(2, asset="161670000531"), master_row(3, asset=None)]
    sap = [sap_row(), sap_row(anln1="161670000531", STORT="C005")]
    s = run(master, sap)["stats"]
    assert (s["master_rows"], s["master_with_asset"], s["matched"]) == (3, 2, 2)
    assert (s["changed_instruments"], s["change_count"]) == (1, 1)
