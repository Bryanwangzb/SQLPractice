import requests
import json
import csv
import pandas as pd



api_key = 'gFiSx3ZxDw13vbmAlSx4K4JEidmV36Mj3UFtjulR'

def fetch_data(url, total_count, params, headers):
    page = 1
    all_deals = []

    while (page - 1) * params['limit'] < total_count:
        params['page'] = page
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_deals.extend(data['deals'])
            page += 1
        else:
            print(f"Failed to fetch data for page {page}: {response.status_code}")
            break

    return all_deals


def extract_custom_fields(deal):
    custom_fields = {}
    for custom in deal.get('dealCustoms', []):
        custom_fields[custom['name']] = custom.get('value') or custom.get('text') or custom.get('number') or custom.get(
            'decimalNumber') or custom.get('selectedValue') or ''
    return custom_fields


def write_to_csv(deals, csv_filename):
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Collect all custom field names
        custom_field_names = set()
        for deal in deals:
            custom_fields = extract_custom_fields(deal)
            custom_field_names.update(custom_fields.keys())

        # Write header
        header = ['ID', 'Name', 'Deal Code', 'Amount', 'Closing Leadtime', 'From Email', 'Memo', 'Created At',
                  'Updated At', 'Reason', 'Total Cost', 'Total Profit', 'Expected Contract Date', 'Deal Type',
                  'Customer Id','Customer', 'Phase', 'Phase Stay Days', 'Product', 'Probability', 'Channel', 'User'] + list(
            custom_field_names)
        csv_writer.writerow(header)

        # Write deal data
        for deal in deals:
            custom_fields = extract_custom_fields(deal)
            row = [
                      deal.get('id'),
                      deal.get('name'),
                      deal.get('dealCode'),
                      deal.get('amount'),
                      deal.get('closingLeadtime'),
                      deal.get('fromEmail'),
                      deal.get('memo'),
                      deal.get('createdAt'),
                      deal.get('updatedAt'),
                      deal.get('reason'),
                      deal.get('totalCost'),
                      deal.get('totalProfit'),
                      deal.get('expectedContractDate'),
                      deal.get('dealType').get('name') if deal.get('dealType') else '',
                      deal.get('customer').get('id') if deal.get('customer') else '',
                      deal.get('customer').get('name') if deal.get('customer') else '',
                      deal.get('phase').get('name') if deal.get('phase') else '',
                      ', '.join(
                          [f"{phase['name']} ({phase['stayDays']} days)" for phase in deal.get('phaseStayDays', [])]),
                      deal.get('product').get('name') if deal.get('product') else '',
                      deal.get('probability').get('name') if deal.get('probability') else '',
                      deal.get('channel').get('name') if deal.get('channel') else '',
                      deal.get('user').get('name') if deal.get('user') else ''
                  ] + [custom_fields.get(field, '') for field in custom_field_names]
            csv_writer.writerow(row)


def main():
    url = 'https://senses-open-api.mazrica.com/v1/deals/'
    params = {
        'limit': 100  # Number of records per page
    }
    headers = {
        'X-API-KEY': api_key
    }

    response = requests.get(url, params=params, headers=headers) # Total number of records to fetch
    total_count = json.loads(response.text)['totalCount']# Total number of records to fetch

    deals = fetch_data(url, total_count, params, headers)
    csv_filename = 'deals_data_tmp.csv'
    write_to_csv(deals, csv_filename)

    df_deal_data = pd.read_csv('deals_data_tmp.csv')
    # User MazricaID_Mapping.xlsx in local directory.
    # df_ivr_name = pd.read_excel('C:\\Users\WorkAccount\Box\MazricaID_Mapping.xlsx')
    df_ivr_name = pd.read_excel('MazricaID_Mapping.xlsx')
    df_ivr_name.columns = ['ivr_company_name', 'Customer Id']
    df_merged = pd.merge(df_deal_data,df_ivr_name,on='Customer Id',how='left')
    # Get IVR company ID by IVR_Companies_Query.csv
    df_ivr_companies_query = pd.read_csv('IVR_Companies_Query.csv')
    df_ivr_companies_query.columns = ['ivr_company_id',
                                      'ivr_company_name']  # rename df_ivr_companies_query's column name for merging with df_merged
    df_merged = pd.merge(df_merged, df_ivr_companies_query, on='ivr_company_name', how='left')
    # Convert ivr_company_id from 'float' to 'String' type.
    df_merged['ivr_company_id'] = df_merged['ivr_company_id'].apply(lambda x: '' if pd.isna(x) else str(int(x)))
    # Fix column
    fix_column = ["ID","Name","Deal Code","Amount","Closing Leadtime","From Email","Memo","Created At","Updated At","Reason","Total Cost","Total Profit","Expected Contract Date",
                  "Deal Type","Customer Id","Customer","Phase","Phase Stay Days","Product","Probability","Channel","User","顧客属性","案件属性","その他備考・メモ","JGC担当窓口",
                  "Cサク担当","希望代理店プラン","案件補足情報（Teamsのスレッドの情報）","顧客課題","[提案面積] ①VLX（単位：m2）","JGC主管部署","[提案面積] ②BLK（単位：m2 | 10m2/点）",
                  "撮影開始予定日","入金","[提案面積] ③その他 （内容および物量を入力）","体験版終了日","事業所","本番環境利用開始日","ivr_company_name","ivr_company_id"]
    df_merged = df_merged[fix_column]

    df_merged.to_csv("deals_data.csv",index=False)



if __name__ == "__main__":
    main()
