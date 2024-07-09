import requests
import zipfile
import os

class Selenium(object):
    __slots__ = ("chromedriver_path")
    os.chdir("../db/")
    def __init__(self):
        self.chromedriver_path = self._chromedriver_init()
# 下载 ChromeDriver

    def _chromedriver_init(self):
        url = "https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.126/mac-x64/chromedriver-mac-x64.zip"
        response = requests.get(url)

        # 将文件保存到本地
        with open("chromedriver-mac-x64.zip", "wb") as file:
            file.write(response.content)

        # 解压缩文件
        with zipfile.ZipFile("chromedriver-mac-x64.zip", "r") as zip_ref:
            zip_ref.extractall("chromedriver_mac")

        # 获取 ChromeDriver 路径
        chromedriver_path = os.path.abspath("chromedriver_mac/chromedriver-mac-x64/chromedriver")
        print(f"ChromeDriver 路径: {chromedriver_path}")
        return chromedriver_path
    
    