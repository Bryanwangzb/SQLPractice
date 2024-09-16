import requests
import pandas as pd
import json
#
# # APIキーと基本URLを設定
# api_key = 'gFiSx3ZxDw13vbmAlSx4K4JEidmV36Mj3UFtjulR'
# base_url = 'https://senses-open-api.mazrica.com/v1/customers'
# headers = {
#     'X-API-Key': api_key
# }
#
# # 結果を格納するリストを初期化
# all_customers = []
#
# # ページネーションを処理
# page = 1
# while True:
#     params = {'page': page}
#     response = requests.get(base_url, headers=headers, params=params)
#
#     if response.status_code != 200:
#         print(f"Error: Received status code {response.status_code}")
#         break
#
#     data = response.json()
#     customers = data.get('customers', [])
#
#     if not customers:
#         # データがなければループを終了
#         break
#
#     # 必要なデータを抽出してリストに追加
#     for customer in customers:
#         customer_id = customer.get('id')
#         customer_name = customer.get('name')
#         all_customers.append({'id': customer_id, 'name': customer_name})
#
#     # 総ページ数に達したかどうかを確認
#     total_count = data.get('totalCount', 0)
#     if len(all_customers) >= total_count:
#         break
#
#     # 次のページに移動
#     page += 1
#
# # データフレームを作成し、CSVファイルに保存
# df = pd.DataFrame(all_customers)
#
# output_file_path = 'customers.csv'
# df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
# df = pd.read_csv('customers.csv')
# df_action = pd.read_csv('actions_data_tmp.csv')
# df_merged = pd.merge(df_action,df,on='customer_name',how='left')
# df_merged.to_csv('action_data_0729.csv',index=False,encoding='utf-8-sig')


#df_mazrica_mapping = pd.read_excel('MazricaID_Mapping.xlsx')

api_key = 'gFiSx3ZxDw13vbmAlSx4K4JEidmV36Mj3UFtjulR'
url = 'https://senses-open-api.mazrica.com/v1/customers'
# リクエスト実行
headers = {
    'X-API-KEY': api_key
}

res = requests.get(url,headers=headers)

# レスポンス内容を辞書型に変換
res = json.loads(res.text)
print(res['totalCount'])

print(f"顧客データを保存しました。")
