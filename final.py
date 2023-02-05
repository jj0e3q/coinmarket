from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
import time
import json
import requests

def collect_json():
    with open('links.txt') as file:
        urls_list = [line.strip() for line in file.readlines()]
    s = requests.Session()
    result_data = []

    for url in urls_list:
        responce = s.get(url=url).json()
        data = responce

        marketPairs = data.get('data').get('marketPairs')
        name_crypto = data.get('data').get('name')
        for pairs in marketPairs:
            result_data.append(
                {
                    "Криптовалюта": name_crypto,
                    "Биржа": pairs.get('exchangeName'),
                    "Торговая пара": pairs.get('marketPair'),
                    "Категория": pairs.get('category'),
                    "Стоимость": f"{pairs.get('price')}",
                    "Последнее обновление": pairs.get('lastUpdated')
                }
            )
        with open("result_data.json", "w", encoding="UTF-16") as file:
            json.dump(result_data, file, indent=4, ensure_ascii=False)

# Аутентификация и создание клиента API Google Sheets
def import_json_to_google_sheets(spreadsheet_id, sheet_name, file_path):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file("service.json", scopes=scopes)
        sheets_service = build('sheets', 'v4', credentials=creds)

        # Откройте файл JSON и прочитайте его содержимое
        with open(file_path, 'r', encoding='UTF-16') as file:
            data = json.load(file)

        # Подготовьте данные для листа
        values = []
        headers = list(data[0].keys())
        values.append(headers)
        for item in data:
            row = []
            for header in headers:
                row.append(item[header])
            values.append(row)

        # Очистить лист
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=sheet_name).execute()

        # Запись данных JSON в лист
        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=sheet_name, valueInputOption='RAW', body={
                "values": values
            }
        ).execute()
        print(f"{len(values)} rows written to sheet '{sheet_name}' in spreadsheet '{spreadsheet_id}'.")
    except HttpError as error:
        print(f"An error occurred: {error}")

while True:
    collect_json()
    import_json_to_google_sheets("ID GOOGLE SHEETS", "Sheet1", "result_data.json")
    time.sleep(300)
