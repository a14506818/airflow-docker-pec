import pyodbc
from pyrfc import Connection
from airflow.exceptions import AirflowFailException

from src.common.common import get_mssql_conn_str, get_sap_conn_params
from src.instrument_sync.config import RFC_NAME, RFC_PARAMS, RFC_TABLE
from src.instrument_sync.rules import SapValidationError, normalize_sap_rows, validate_sap_rows


def get_sap_assets(**context):
    """
    F1 擷取 SAP 資產主檔 + F2 資料檢核
    連線失敗 → 一般例外，依 DAG retries 重試；檢核不通過 → AirflowFailException，直接失敗不重試
    """
    conn = Connection(**get_sap_conn_params())
    try:
        result = conn.call(RFC_NAME, **RFC_PARAMS)
    finally:
        conn.close()

    rows = normalize_sap_rows(result.get(RFC_TABLE, []))
    print(f"✅ SAP {RFC_NAME} 回傳 {len(rows)} 筆")

    try:
        validate_sap_rows(rows)
    except SapValidationError as e:
        raise AirflowFailException(f"❌ SAP 資料檢核不通過，同步中止、主檔不異動：{e}")

    print("✅ SAP 資料檢核通過")
    return rows  # list[dict]，XCom 可直接傳


def get_hr_members(**context):
    """F4 人事資料：BPMDB.dbo.BPMSysUsers（含離職帳號，由 rules 判斷）"""
    conn = pyodbc.connect(get_mssql_conn_str(), timeout=5)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Account, DisplayName, SAP_Account, Disabled
            FROM BPMDB.dbo.BPMSysUsers
        """)
        rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        raise ValueError("❌ BPMSysUsers 查無資料，請檢查資料庫")

    members = [
        {
            "Account": r.Account,
            "DisplayName": r.DisplayName,
            "SAP_Account": r.SAP_Account,
            "Disabled": bool(r.Disabled),  # 離職／停用；DateResigned 全為 1900-01-01，不使用
        }
        for r in rows
    ]
    print(f"✅ 取得 BPMSysUsers {len(members)} 筆")
    return members
