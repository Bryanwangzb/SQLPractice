import aiohttp
import asyncio
import pandas as pd
import requests
import json

# Output file path
powerBI_csv_analysis = 'C:\\Users\\Administrator\\Documents\\PowerBI_CSV\\data_analysis'

# APIのURL
API_URL = "https://senses-open-api.mazrica.com/v1/actions"
DEAL_URL = "https://senses-open-api.mazrica.com/v1/deals"
api_key = 'gFiSx3ZxDw13vbmAlSx4K4JEidmV36Mj3UFtjulR'  # Replace with your actual API key
PER_PAGE = 100
# TOTAL_COUNT = 952  # 総件数

headers = {
    'X-API-KEY': api_key
}

response = requests.get(API_URL, headers=headers)  # Total number of records to fetch
TOTAL_COUNT = json.loads(response.text)['totalCount']  # Total number of records to fetch



async def fetch_page(session, page):
    params = {"page": page, "perPage": PER_PAGE}

    async with session.get(API_URL, params=params,headers=headers) as response:
        return await response.json()


async def fetch_all_data():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for page in range(1, (TOTAL_COUNT // PER_PAGE) + 2):  # ページ数を計算
            tasks.append(fetch_page(session, page))
        pages = await asyncio.gather(*tasks)
        return pages


# メイン処理
async def main():
    pages = await fetch_all_data()

    all_data = []
    for page_data in pages:
        all_data.extend(page_data['actions'])

    # 必要なkey-valueペアを抽出してリストを作成
    extracted_data = []
    for action in all_data:
        extracted_data.append({
            "id": action["id"],
            "startDatetime": action["startDatetime"],
            "createdAt": action["createdAt"],
            "updatedAt": action["updatedAt"],
            "endDatetime": action["endDatetime"],
            "active": action["active"],
            "syncCalendar": action["syncCalendar"],
            "preNote": action["preNote"],
            "result": action["result"],
            "extraResult": action["extraResult"],
            "pattern_id": action["pattern"]["id"],
            "pattern_name": action["pattern"]["name"],
            "deal_id": action["deal"]["id"],
            "deal_name": action["deal"]["name"],
            "purpose_id": action["purpose"]["id"],
            "purpose_name": action["purpose"]["name"],
            "creator_id": action["creator"]["id"],
            "creator_name": action["creator"]["name"],
            "invitees": ", ".join([invitee["name"] for invitee in action.get("invitees", [])])
        })

    # DataFrameに変換
    df = pd.DataFrame(extracted_data)

    # deal_data.csvからcustomer関連情報を抽出
    df_ivr_customer_info = pd.read_csv(powerBI_csv_analysis + "\\deals_data.csv",usecols=["ID","Customer Id","Customer","ivr_company_name","ivr_company_id"])
    df_ivr_customer_info.columns=["deal_id","Customer Id","Customer Name","ivr_company_name","ivr_company_id"]  # rename df_ivr_companies_query's column name for merging with df_merged
    df = pd.merge(df,df_ivr_customer_info,on="deal_id",how="left")
    fix_column = ["id","startDatetime", "createdAt", "updatedAt", "endDatetime", "active", "syncCalendar","preNote",
                  "result", "extraResult", "pattern_id", "pattern_name", "deal_id", "deal_name", "purpose_id",
                  "purpose_name", "creator_id", "creator_name", "invitees", "Customer Id", "Customer Name",
                  "ivr_company_name", "ivr_company_id"]

    df = df[fix_column]
    # CSVファイルに出力
    df.to_csv(powerBI_csv_analysis + "\\actions_data.csv", index=False, encoding='utf-8-sig')

    print("データの取得とCSVファイルの作成が完了しました。")


# 実行
asyncio.run(main())
