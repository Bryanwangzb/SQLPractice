import subprocess
import os
import sys

sys.path.append('C:\\Users\WorkAccount\AppData\Local\Programs\Python\Python312\Lib\site-packages')
def run_scripts_and_delete_csvs(scripts, csv_files):
    """
    指定されたPythonスクリプトを順番に実行し、全てのスクリプトが正常に終了したら複数のCSVファイルを削除する関数

    Args:
    - scripts: 実行するPythonスクリプトのリスト（順番通り）
    - csv_files: 削除するCSVファイルのパスのリスト
    """
    try:
        # 各スクリプトを順番に実行
        for script in scripts:
            result = subprocess.run(["python", script], check=True)
            print(f"{script} が正常に実行されました。")

        # すべてのスクリプトが成功したらCSVファイルを削除
        for csv_file in csv_files:
            if os.path.exists(csv_file):
                os.remove(csv_file)
                print(f"{csv_file} を削除しました。")
            else:
                print(f"{csv_file} が見つかりませんでした。")

    except subprocess.CalledProcessError as e:
        print(f"スクリプトの実行中にエラーが発生しました: {e}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")

# 使用例
scripts = ["getIVRCompanyInfo.py", "getMazricaInfoCustomer.py", "getMazricaInfoDeal.py", "getMazricaInfoAction.py"]
csv_files = ["customers_data_tmp.csv", "deals_data_tmp.csv"]

run_scripts_and_delete_csvs(scripts, csv_files)
