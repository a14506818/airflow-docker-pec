import os
import pandas as pd
from dotenv import load_dotenv
from pyrfc import Connection
from datetime import date, datetime
from decimal import Decimal

def get_sap_fx(PI_DATE, PI_FROM_CURR, PI_TO_CURR):
    load_dotenv()  # 載入 .env 檔
    # 連線資訊可放在 .env 或 config 中 
    conn_params = {
        "user": os.getenv("SAP_USER"),
        "passwd": os.getenv("SAP_PASS"),
        "ashost": os.getenv("SAP_ASHOST"),
        "sysnr": os.getenv("SAP_SYSNR"),
        "client": os.getenv("SAP_CLIENT"),
        "lang": os.getenv("SAP_LANG", "EN"),
    }
    conn = Connection(**conn_params)

    try:
        # 呼叫 RFC
        rfc_result = conn.call('Z_FI_BPM_005', PI_DATE=PI_DATE, PI_FROM_CURR=PI_FROM_CURR, PI_TO_CURR=PI_TO_CURR)
        print("rfc_result: ", rfc_result)
        df = pd.DataFrame([rfc_result])
        print("✅ SAP 匯率資料如下：")
        print(df.head())
        # clean data
        df = df.rename(columns={
            "PE_FROM_FACTOR": "from_ratio",
            "PE_KURSF": "fx_rate",
            "PE_TO_FACTOR": "to_ratio"
        })
    except Exception as e:
        print("❌ 呼叫 SAP RFC 發生錯誤(no data)：", str(e))
        df = pd.DataFrame([{"from_ratio": 1, "fx_rate": 0, "to_ratio": 1}])
    finally:    
        df["from_curr"] = PI_FROM_CURR
        df["to_curr"] = PI_TO_CURR

    return df

def clean_data_for_sap(rate_type=None, **context):
    if rate_type not in ['M','V']:
        raise ValueError("❌ Please define rate type ! M or V")

    ti = context["ti"] # 取得 Task Instance
    fx_dict = ti.xcom_pull(task_ids="crawl_cpt_fx")
    crawl_df = pd.DataFrame(fx_dict)
    if crawl_df.empty:
        raise ValueError("❌ 轉換成 DataFrame 後為空，請檢查上游任務")
    print("✅ 成功取得xcom，前幾筆資料如下：")
    print(crawl_df.head())

    # join SAP fx data ----------------------------------------------------------------------------------------
    drop_indices = []
    for idx, row in crawl_df.iterrows():
        # get SAP fx data
        sap_fx_df = get_sap_fx(date.today(),row["from_curr"],"TWD")
        from_ratio = sap_fx_df["from_ratio"].iloc[0]
        if sap_fx_df["fx_rate"].iloc[0] == 0:
            drop_indices.append(idx)
        else:
            crawl_df.at[idx, "from_ratio"] = from_ratio # join SAP ratio
    # remove curr if not in SAP
    crawl_df.drop(index=drop_indices, inplace=True)
    crawl_df.reset_index(drop=True, inplace=True)

    # fx rate * ratio
    # 確保欄位轉成 Decimal
    crawl_df["fx_rate"] = crawl_df["fx_rate"].apply(lambda x: Decimal(str(x)))
    crawl_df["from_ratio"] = crawl_df["from_ratio"].apply(lambda x: Decimal(str(x)))
    crawl_df["fx_rate_with_ratio"] = crawl_df["fx_rate"] * crawl_df["from_ratio"]

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
    format_df["rate_type"] = rate_type
    format_df["VALID_FROM"] = format_df["VALID_FROM"].apply(lambda x: datetime.strptime(x, "%Y%m%d").date())
    print("✅ Data ReFormat：")
    print(format_df.head())

    return format_df.to_dict("records")  # ❗XCom 不支援直接傳 df，要先轉成 dict

def clean_data_for_bpm():
    pass

