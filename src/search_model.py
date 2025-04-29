
import os
from datetime import date
import json

class SearchModel:
    def __init__(self) -> None:
        self.患者名= ""
        self.住院号 = ""
        self.入院时间 = date.today().strftime('%Y-%m-%d')
        self.出院时间 = date.today().strftime('%Y-%m-%d')
        self.文件类型=''
        self.文件目录=''
        self.文件名称=''
        self.页号=0
        self.页内容=''

    def parse_fname(self,path,fname):
        txt_list = fname.split("_")
        print(txt_list)
        self.患者名=""
        self.住院号=txt_list[0]
        self.文件类型=txt_list[1]
        self.文件目录=""
        self.文件名称=fname

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4,ensure_ascii=False)