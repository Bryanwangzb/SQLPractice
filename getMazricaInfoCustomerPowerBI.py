import pandas as pd
import json
import requests
import csv
import datetime
import sys


# Output file path
powerBI_csv_analysis = 'C:\\Users\\Administrator\\Documents\\PowerBI_CSV\\data_analysis'

api_key = 'gFiSx3ZxDw13vbmAlSx4K4JEidmV36Mj3UFtjulR'

def fetch_data(url, total_count, params, headers):
    page = 1
    all_customers = []

    while (page - 1) * params['limit'] < total_count:
        params['page'] = page
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_customers.extend(data['customers'])
            page += 1
        else:
            print(f"Failed to fetch data for page {page}: {response.status_code}")
            break

    return all_customers


def extract_custom_fields(customer):
    custom_fields = {}
    for custom in customer.get('customerCustoms', []):
        custom_fields[custom['name']] = custom.get('value') or custom.get('text') or custom.get('number') or custom.get(
            'decimalNumber') or custom.get('selectedValue') or ''
    return custom_fields


def write_to_csv(customers, csv_filename):
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Collect all custom field names
        custom_field_names = set()
        for customer in customers:
            custom_fields = extract_custom_fields(customer)
            custom_field_names.update(custom_fields.keys())

        # Write header
        header = ['id','name','address','telNo','webUrl','employee','closingMonth','createdAt','updatedAt','capital','ownerRole']+list(custom_field_names)
        csv_writer.writerow(header)

        # Write customer data
        for customer in customers:
            custom_fields = extract_custom_fields(customer)
            row = [
                      customer.get('id'),
                      customer.get('name'),
                      customer.get('address'),
                      customer.get('telNo'),
                      customer.get('webUrl'),
                      customer.get('employee'),
                      customer.get('closingMonth'),
                      customer.get('createdAt'),
                      customer.get('updatedAt'),
                      customer.get('capital'),
                      customer.get('ownerRole')
                  ] + [custom_fields.get(field, '') for field in custom_field_names]
            csv_writer.writerow(row)


def main():
    url = 'https://senses-open-api.mazrica.com/v1/customers'
    params = {
        'limit': 100  # Number of records per page
    }
    headers = {
        'X-API-KEY': api_key
    }
    # Total number of records to fetch

    response = requests.get(url, params=params, headers=headers)
    total_count = json.loads(response.text)['totalCount']# Total number of records to fetch
    customers = fetch_data(url, total_count, params, headers)
    csv_filename = powerBI_csv_analysis + '\\customers_data_tmp.csv'
    write_to_csv(customers, csv_filename)

    # Get IVR company name by MaziricaID_Mapping.xlsx
    df_customer_data = pd.read_csv(powerBI_csv_analysis + '\\customers_data_tmp.csv')
    # User MazricaID_Mapping in local directory
    # df_ivr_name = pd.read_excel('C:\\Users\WorkAccount\Box\MazricaID_Mapping.xlsx')
    df_ivr_name = pd.read_excel('MazricaID_Mapping.xlsx')
    df_merged = pd.merge(df_customer_data,df_ivr_name,on='id',how='left')
    # Get IVR company ID by IVR_Companies_Query.csv
    df_ivr_companies_query = pd.read_csv('IVR_Companies_Query.csv')
    df_ivr_companies_query.columns= ['ivr_company_id', 'ivr_company_name']  # rename df_ivr_companies_query's column name for merging with df_merged
    df_merged = pd.merge(df_merged,df_ivr_companies_query,on='ivr_company_name',how='left')
    # Convert ivr_company_id from 'float' to 'String' type.
    df_merged['ivr_company_id'] = df_merged['ivr_company_id'].apply(lambda x: '' if pd.isna(x) else str(int(x)))
    #fix column
    fix_column = ["id","name","address","telNo", "webUrl", "employee","closingMonth","createdAt","updatedAt","capital", "ownerRole",
                  "利用状況", "契約プラン(ID)","事業所総面積（平米）","契約プラン(面積)", "メモ・備考", "A-MIS導入有無", "請求書送付先",
                "契約開始日(以降は変更しない) / トライアル開始日 ", "キーマン メールアドレス","契約満了日 / トライアル終了日","キーマン",
                "ivr_company_name", "ivr_company_id"]
    df_merged = df_merged[fix_column]
    df_merged.to_csv(powerBI_csv_analysis + '\\customer_data.csv',index=False)



if __name__ == "__main__":
    main()
