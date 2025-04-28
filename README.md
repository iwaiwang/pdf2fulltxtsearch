# pdf2fulltxtsearch

### 遍历pdf的目录，把pdf的内容获取出来后放入opensearch的索引中。
运行main.py，选择pdf的目录，点击扫描按钮就可以自动的扫描pdf，并把pdf的内容放入opensearch的索引中。
扫描过的pdf文件会记录在本地的sqlite数据库中
pdf的内容按页来存放，索引的格式如下：
```python
   index = {
            'mappings': {
                'properties': {
                    '患者名': {'type': 'keyword'},
                    '住院号': {'type': 'keyword'},
                    '住院日期': {'type': 'date'},
                    '出院日期': {'type': 'date'},
                    '文件类型': {'type': 'keyword'},
                    '文件目录': {'type': 'keyword'},
                    '文件名称': {'type': 'keyword'},
                    '页号': {'type': 'keyword'},
                    '页内容': {
                        'type': 'text',
                        'analyzer': 'ik_max_word',
                        'search_analyzer': 'ik_smart'
                    }
                }
            }
        }
```

### 准备python环境，最好用venv来建立本地环境

``` bash
python -m venv .venv
```

激活本地环境
``` bash
source .venv/bin/activate  （macos)
.venv/Scripts/activate.bat  （windows）
``` 


安装python想管的库
```bash
pip install -r requirements.txt

```

### 运行程序
.venv/bin/python main.py  (macos)
.venv/Scirpt/python main.py (windows)

