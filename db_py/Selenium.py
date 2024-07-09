import requests
import zipfile
import os

class Selenium(object):
    
    def __init__(self):
        pass
# 下载 ChromeDriver
url = "https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.126/mac-x64/chromedriver-mac-x64.zip"
response = requests.get(url)

# 将文件保存到本地
with open("../db/chromedriver-mac-x64.zip", "wb") as file:
    file.write(response.content)

# 解压缩文件
with zipfile.ZipFile("../db/chromedriver-mac-x64.zip", "r") as zip_ref:
    zip_ref.extractall("../db/chromedriver_mac")

# 获取 ChromeDriver 路径
chromedriver_path = os.path.abspath("../db/chromedriver_mac/chromedriver-mac-x64/chromedriver")
print(f"ChromeDriver 路径: {chromedriver_path}")
