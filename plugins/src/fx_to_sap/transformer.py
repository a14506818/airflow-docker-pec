import os
import pandas as pd
from airflow.models import Variable
from dotenv import load_dotenv
from pyrfc import Connection
from datetime import date, datetime
from decimal import Decimal

from src.common.common import get_mssql_conn_str, get_sap_conn_params

def gen_fx_to_other_currencies(skipped=False, **context): # ---------------------------------------------------------------------------------------------------------------------
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    fx_dict = ti.xcom_pull(task_ids="crawl_fx_data")
    crawl_df = pd.DataFrame(fx_dict)
    if crawl_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    print(crawl_df.head())

    # skipped, pass xcom to next task
    if skipped:
        print("⚠️ Skipped gen_fx_to_other_currencies, pass xcom to next task")
        return crawl_df.to_dict("records") 


    to_currencies = Variable.get("fx_target_currency_list", deserialize_json=True) # 從 Airflow 變數中取得目標幣別清單
    # remove TWD, 因為 TWD 是基準幣別
    to_currencies = [curr for curr in to_currencies if curr != "TWD"]
    print("to_currencies:", to_currencies)

    fx_to_other_currency_df = pd.DataFrame()  # 初始化空的 DataFrame 用於存放轉換後的資料
    for other_currency in to_currencies:
        other_currency_to_twd_row = crawl_df[crawl_df["from_curr"] == other_currency]
        if other_currency_to_twd_row.empty:
            raise ValueError(f"❌ 找不到 {other_currency} ➝ TWD 匯率")

        other_currency_to_twd = other_currency_to_twd_row["fx_rate"].values[0]

        temp_df = crawl_df.copy()
        # 計算 from_curr ➝ USD 的匯率
        temp_df["fx_rate"] = temp_df["sellValue"].apply(lambda x: Decimal(str(x)) / Decimal(str(other_currency_to_twd)))
        temp_df["to_curr"] = other_currency
        print("❗ 轉換成其他幣別的匯率：")
        print(temp_df[["from_curr", "to_curr", "fx_rate"]].head())

        # 
        fx_to_other_currency_df = pd.concat([fx_to_other_currency_df, temp_df], ignore_index=True)

    concat_df = pd.concat([crawl_df, fx_to_other_currency_df], ignore_index=True)
    # 排除自己轉自己
    concat_df = concat_df[concat_df["from_curr"] != concat_df["to_curr"]]
    print(f"✅ Convert to other currencies, concat with origin data：")
    print(concat_df)
    
    return concat_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def get_sap_fx(PI_DATE, PI_FROM_CURR, PI_TO_CURR): # ---------------------------------------------------------------------------------------------------------------------
    conn_params = get_sap_conn_params()
    conn = Connection(**conn_params)
    try:
        # 呼叫 RFC
        rfc_result = conn.call('Z_FI_BPM_005', PI_DATE=PI_DATE, PI_FROM_CURR=PI_FROM_CURR, PI_TO_CURR=PI_TO_CURR)
        print("rfc_result: ", rfc_result)
        df = pd.DataFrame([rfc_result])
        print(f"✅ SAP 匯率資料如下：")
        print(df.head())
        # clean data
        df = df.rename(columns={
            "PE_FROM_FACTOR": "from_ratio",
            "PE_KURSF": "fx_rate",
            "PE_TO_FACTOR": "to_ratio"
        })
    except Exception as e:
        print("❌ 呼叫 SAP RFC 發生錯誤(no data)：", str(e))
        print(f"{PI_FROM_CURR} ➝ {PI_TO_CURR} 的 SAP 匯率資料不存在，使用預設值")
        df = pd.DataFrame([{"from_ratio": 1, "fx_rate": 0, "to_ratio": 1}])
    finally:    
        df["from_curr"] = PI_FROM_CURR
        df["to_curr"] = PI_TO_CURR

    return df

def clean_data_for_sap(rate_type=None, **context): # ---------------------------------------------------------------------------------------------------------------------
    if rate_type not in ['M','V']:
        raise ValueError("❌ Please define rate type ! M or V")

    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    fx_dict = ti.xcom_pull(task_ids="gen_fx_to_other_currencies")
    crawl_df = pd.DataFrame(fx_dict)
    if crawl_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    print(crawl_df.head())

    # join SAP fx data ----------------------------------------------------------------------------------------
    drop_indices = []
    for idx, row in crawl_df.iterrows():
        # get SAP fx data
        sap_fx_df = get_sap_fx(date.today(),row["from_curr"],row["to_curr"])
        from_ratio = sap_fx_df["from_ratio"].iloc[0]
        to_ratio = sap_fx_df["to_ratio"].iloc[0]
        if sap_fx_df["fx_rate"].iloc[0] == 0:
            drop_indices.append(idx)
        
        crawl_df.at[idx, "from_ratio"] = from_ratio # join SAP ratio
        crawl_df.at[idx, "to_ratio"] = to_ratio

    # remove curr if not in SAP 
    crawl_df.drop(index=drop_indices, inplace=True)
    crawl_df.reset_index(drop=True, inplace=True)

    # set 'end' always to the end of month
    crawl_df["end"] = crawl_df["end"].apply(lambda x: datetime.strptime(x, "%Y%m%d").replace(day=1) + pd.offsets.MonthEnd(0)).dt.strftime("%Y%m%d")

    # fx rate * ratio; round to 5 nums
    # 確保欄位轉成 Decimal
    crawl_df["fx_rate"] = crawl_df["fx_rate"].apply(lambda x: Decimal(str(x)))
    crawl_df["from_ratio"] = crawl_df["from_ratio"].apply(lambda x: Decimal(str(x)))
    crawl_df["fx_rate_with_ratio"] = crawl_df["fx_rate"] * crawl_df["from_ratio"]
    crawl_df["fx_rate_with_ratio"] = crawl_df["fx_rate_with_ratio"].apply(lambda x: round(float(x), 5))

    print("✅ Data Cleaned：")
    print(crawl_df.head())

    # ReFormat ------------------------------------------------------------------------------------------------
    # set date col for different rate type
    valid_from = ""
    if rate_type == "M":
        valid_from = "start"
    elif rate_type == "V":
        valid_from = "end"

    format_df = crawl_df[["from_curr", "to_curr", "fx_rate_with_ratio", valid_from]]
    format_df = format_df.rename(columns={
        "from_curr": "FROM_CURR",
        "to_curr": "TO_CURRNCY",
        "fx_rate_with_ratio": "EXCH_RATE",
        valid_from: "VALID_FROM",
    })
    format_df["RATE_TYPE"] = rate_type
    format_df["VALID_FROM"] = format_df["VALID_FROM"].apply(lambda x: datetime.strptime(x, "%Y%m%d").date())

    # final data type conversion --------------------------------------------------------------------------------------
    format_df["RATE_TYPE"] = format_df["RATE_TYPE"].astype(str)
    format_df["FROM_CURR"] = format_df["FROM_CURR"].astype(str)
    format_df["TO_CURRNCY"] = format_df["TO_CURRNCY"].astype(str)
    format_df["EXCH_RATE"] = format_df["EXCH_RATE"].apply(lambda x: f"{float(x):.5f}")  # 強制轉字串
    format_df["VALID_FROM"] = format_df["VALID_FROM"].apply(lambda x: x.strftime("%Y%m%d"))  # 轉成 YYYYMMDD 字串
    # 強迫填寫欄位 (sap table 的所有欄位都需要出現 且小數點精度必須正確) ---------------------------------------------------------
    format_df["FROM_FACTOR"] = "1"  
    format_df["TO_FACTOR"] = "1"
    format_df["EXCH_RATE_V"] = "1.00000"
    format_df["FROM_FACTOR_V"] = "1"
    format_df["TO_FACTOR_V"] = "1"

    print("✅ Data ReFormat：")
    print(format_df)

    return format_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def clean_data_for_bpm(**context): # ---------------------------------------------------------------------------------------------------------------------
    # get XCOM -----------------------------------------------------------------------------------------------
    ti = context["ti"] # 取得 Task Instance
    fx_dict = ti.xcom_pull(task_ids="crawl_fx_data")
    crawl_df = pd.DataFrame(fx_dict)
    if crawl_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    print(crawl_df.head())

    # remove USD, only keep TWD --------------------------------------------------------------------------------
    crawl_df = crawl_df[crawl_df["to_curr"] == "TWD"]
    if crawl_df.empty:
        raise ValueError("❌ 沒有 TWD 的匯率資料，請檢查上游任務")

    # ReFormat ------------------------------------------------------------------------------------------------
    format_df = crawl_df[["from_curr", "to_curr", "fx_rate", "start"]]
    format_df = format_df.rename(columns={
        "from_curr": "Currency",
        "to_curr": "Currency2",
        "fx_rate": "Rate",
        "start": "RateDate",
    })
    # final data type conversion --------------------------------------------------------------------------------------
    format_df["Currency"] = format_df["Currency"].astype(str)
    format_df["Currency2"] = format_df["Currency2"].astype(str)
    format_df["Rate"] = format_df["Rate"].apply(lambda x: f"{float(x):.5f}")  # 強制轉字串
    format_df["RateDate"] = format_df["RateDate"].apply(lambda x: datetime.strptime(x, "%Y%m%d").date().strftime("%Y-%m-%d"))

    print("✅ Data ReFormat：")
    print(format_df)

    return format_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

