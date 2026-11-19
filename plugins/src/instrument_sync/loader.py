import pandas as pd
import pendulum
import pyodbc

from src.common.common import get_mssql_conn_str
from src.instrument_sync.config import (
    MASTER_READ_COLUMNS, MASTER_TABLE, SESSION_CONTEXT_KEY, SOURCE, WRITABLE_COLUMNS,
)
from src.instrument_sync.report import SHEETS, build_summary_html, summary_rows
from src.instrument_sync.rules import compute_sync


def is_dry_run(params):
    value = params.get("dry_run", False) if params else False
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "y", "yes")
    return bool(value)


def read_master(cursor, lock=False):
    """讀主檔；正式同步時加 UPDLOCK，讀到寫之間不讓別人改同一筆"""
    cols = ", ".join(f"[{c}]" for c in MASTER_READ_COLUMNS)
    hint = " WITH (UPDLOCK)" if lock else ""
    cursor.execute(f"SELECT {cols} FROM {MASTER_TABLE}{hint}")
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def write_updates(conn, updates, synced, now_str):
    """
    F3 寫入：單一交易，任一筆失敗全部還原
    updates: {Instrument_No: {欄位: 新值}}；synced: 要更新 Last_Sync_Date 的 Instrument_No
    """
    cursor = conn.cursor()
    try:
        # 讓 trigger 把異動來源記為 SAP_SYNC
        cursor.execute("EXEC sp_set_session_context @key = ?, @value = ?", SESSION_CONTEXT_KEY, SOURCE)

        for instrument_no, cols in updates.items():
            bad = set(cols) - WRITABLE_COLUMNS
            if bad:
                raise RuntimeError(f"嘗試寫入白名單以外的欄位：{sorted(bad)}")
            names = sorted(cols)
            set_sql = ", ".join(f"[{c}] = ?" for c in names)
            cursor.execute(
                f"UPDATE {MASTER_TABLE} SET {set_sql}, [Updated_By] = ? WHERE [Instrument_No] = ?",
                *[cols[c] for c in names], SOURCE, instrument_no,
            )

        # Last_Sync_Date 在 trigger 的 @Skip 內：不產生 log，也不會蓋 Updated_Date
        for instrument_no in synced:
            cursor.execute(
                f"UPDATE {MASTER_TABLE} SET [Last_Sync_Date] = ? WHERE [Instrument_No] = ?",
                now_str, instrument_no,
            )

        conn.commit()
    except Exception:
        conn.rollback()
        print("❌ 寫入失敗，已全部還原")
        raise


def sync_master(**context):
    """F3 差異比對與更新 + F4 保管人同步；dry-run 只比對不寫入"""
    ti = context["ti"]
    dry_run = is_dry_run(context.get("params"))
    sap_rows = ti.xcom_pull(task_ids="get_sap_assets")
    members = ti.xcom_pull(task_ids="get_hr_members")
    if not sap_rows or not members:
        raise ValueError("❌ 取不到 SAP 或人事資料，請檢查上游任務")

    now_str = pendulum.now("Asia/Taipei").format("YYYY/MM/DD HH:mm")
    conn = pyodbc.connect(get_mssql_conn_str(), timeout=5)  # autocommit=False
    try:
        master = read_master(conn.cursor(), lock=not dry_run)
        result = compute_sync(master, sap_rows, members)
        stats = result["stats"]
        print(f"✅ 比對完成：{stats}")

        if dry_run:
            conn.rollback()
            print("⚠️ Dry-run：不寫入主檔")
        else:
            write_updates(conn, result["updates"], result["synced"], now_str)
            print(f"✅ 寫入完成：異動 {stats['changed_instruments']} 筆儀器、"
                  f"{stats['change_count']} 個欄位；更新同步時間 {len(result['synced'])} 筆")
    finally:
        conn.close()

    report = {k: v for k, v in result.items() if k not in ("updates", "synced")}
    report["mode"] = "DRY_RUN" if dry_run else "SYNC"
    report["run_time"] = now_str
    return report


def gen_attchments(**context):
    """F5 每日報表：Excel 附件 + Email 內文（XCom key summary_html，由 on_success 帶入信件）"""
    ti = context["ti"]
    report = ti.xcom_pull(task_ids="sync_master")
    if not report:
        raise ValueError("❌ 取不到同步結果，請檢查上游任務")

    start_date = context["dag_run"].start_date
    if not isinstance(start_date, pendulum.DateTime):
        start_date = pendulum.instance(start_date)
    local_time = start_date.in_timezone("Asia/Taipei")
    suffix = "__DRYRUN" if report.get("mode") == "DRY_RUN" else ""
    file_name = f"FinalData__{context['dag'].dag_id}__{local_time.strftime('%Y%m%d_%H%M')}{suffix}.xlsx"
    file_path = f"/opt/airflow/export/{file_name}"

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows(report), columns=["項目", "值"]).to_excel(
            writer, sheet_name="執行摘要", index=False)
        for title, key, columns, _ in SHEETS:
            rows = report.get(key, [])
            df = pd.DataFrame([[r.get(k, "") for k, _ in columns] for r in rows],
                              columns=[label for _, label in columns])
            df.to_excel(writer, sheet_name=title, index=False)
    print(f"✅ 成功寫入 Excel 檔案: {file_path}")

    ti.xcom_push(key="summary_html", value=build_summary_html(report))
    return [file_path]
