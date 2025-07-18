import os
import pandas as pd

import pendulum
from datetime import datetime, date

def compare_member_data(**context):
    """
    比對 UAT 和 PRD 的會員資料
    """
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    member_data_UAT = ti.xcom_pull(task_ids="get_UAT_member_data")
    member_data_UAT_df = pd.DataFrame(member_data_UAT)

    member_data_PRD = ti.xcom_pull(task_ids="get_PRD_member_data")
    member_data_PRD_df = pd.DataFrame(member_data_PRD)

    if member_data_UAT_df.empty or member_data_PRD_df.empty:
        raise ValueError("❌ UAT 或 PRD 的會員資料為空，請檢查上游任務")

    # df nan 處理 
    member_data_UAT_df.fillna("", inplace=True)
    member_data_PRD_df.fillna("", inplace=True)

    # Merge DataFrames --------------------------------------------------------------------------------------
    merged = pd.merge(
        member_data_UAT_df, 
        member_data_PRD_df, 
        on="FullMemberName", 
        suffixes=('_UAT', '_PRD'),
        how='outer', # 'left', 'right', 'outer', 'inner' 可選，這裡使用 'left' 以保留 UAT 的所有資料 
        indicator=True
    )

    # nan 處理
    merged.fillna({col: "" for col in merged.select_dtypes(exclude='category').columns}, inplace=True)

     # 要比對的欄位
    compare_columns = [
        "UserAccount", "DisplayName", "HRID", "LeaderTitle", "SupervisorUserAccount", "UserCostCenter",
        "PEC_Level", "CMO_COO_CTO", "GM", "CEO", "Chairman", "OU_Code", "OU_CostCenter",
        "OUName", "Manager", "company"
    ]

    # 差異細節 DataFrame
    diff_detail = pd.DataFrame()
    diff_detail['FullMemberName'] = merged['FullMemberName']
    diff_detail['Source'] = merged['_merge']  # both, left_only, right_only

    for col in compare_columns:
        col_UAT = f"{col}_UAT"
        col_PRD = f"{col}_PRD"

        # 有些欄位可能缺少，先確認存在
        if col_UAT in merged.columns and col_PRD in merged.columns:
            diff_mask = merged[col_UAT] != merged[col_PRD]
            diff_mask = diff_mask | (merged[col_UAT].isna() != merged[col_PRD].isna())

            # 建立 "舊值 → 新值" 欄位
            diff_detail[col] = merged.apply(
                lambda row: f"{row[col_UAT]} → {row[col_PRD]}" if diff_mask.loc[row.name] else "",
                axis=1
            )

    # 過濾出有差異的行
    diff_detail = diff_detail[diff_detail.apply(lambda row: any(row[col] for col in compare_columns), axis=1)]

    print("✅ 差異細節表：")
    print(diff_detail)

    return diff_detail.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dictq

def gen_attchments(**context):
    """
    生成差異細節的附件
    """
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    data = ti.xcom_pull(task_ids="compare_member_data")
    df = pd.DataFrame(data)

    if not df.empty:
        print("✅ 成功取得差異細節資料")
        print(df)
    else:
        raise ValueError("❌ 差異細節資料為空，請檢查上游任務")

    # 分割資料為三個 DataFrame：左側獨有、右側獨有、兩側都有
    left_only_df = df[df['Source'] == 'left_only']
    right_only_df = df[df['Source'] == 'right_only']
    both_df = df[df['Source'] == 'both']

    # 檔名與路徑
    tz = pendulum.timezone("Asia/Taipei")
    local_time = context["execution_date"].in_timezone(tz)
    file_name = f"FinalData__{context['dag'].dag_id}__{local_time.strftime('%Y%m%d_%H%M')}.xlsx"
    file_path = f"/opt/airflow/export/{file_name}"

    print("爬蟲結束，開始匯出結果至Excel...")

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        sheet_written = False

        if not left_only_df.empty:
            left_only_df.to_excel(writer, sheet_name="Left_UAT請刪除", index=False)
            sheet_written = True

        if not right_only_df.empty:
            right_only_df.to_excel(writer, sheet_name="Right_UAT請新增", index=False)
            sheet_written = True

        if not both_df.empty:
            both_df.to_excel(writer, sheet_name="Both_UAT更新資料", index=False)
            sheet_written = True

        # 如果都沒資料，至少寫入一個空 sheet
        if not sheet_written:
            pd.DataFrame({"empty": []}).to_excel(writer, sheet_name="EMPTY", index=False)
            print("❌ 查無任何結果，已生成空白 sheet。")
        else:
            print(f"✅ 成功寫入 Excel 檔案: {file_path}")


    file_path_list = [file_path]  # 將檔案路徑放入列表中
    return file_path_list