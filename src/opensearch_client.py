import logging
import json

# 将 elasticsearch 导入改为 opensearchpy
from opensearchpy import OpenSearch
# 导入 OpenSearch 的异常类，并根据需要改名以区分
from opensearchpy.exceptions import ConnectionError as OSConnectionError, RequestError
from sys_config import SysConfig

# ... 其他导入和日志设置
# 设置日志
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 将类名从 ESClient 改为 OSClient
class OSClient:
    # 初始化方法
    def __init__(self, config): # 注意: 配置文件名可能仍沿用旧的，您可以根据需要修改
        self.os = None # 将 es 变量改为 os_client
        self.index_name = None

        try:
            self.index_name = config["index_name"]
            # 修改日志信息
            logger.info(f"Attempting to connect to OpenSearch at {config['host']}...")
            # 将 Elasticsearch 类改为 OpenSearch
            self.os = OpenSearch(
                hosts=config["host"],
                http_auth=(config["user"], config["password"]),
                # === 解决 SSL 证书验证失败的核心代码 ===
                # 警告: 在生产环境或安全敏感环境不推荐使用此设置
                use_ssl=False,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False,
                # ======================================
            )

            # # 使用新的客户端变量进行 ping
            if not self.os.ping():
                  # 使用导入的 OSConnectionError
                raise OSConnectionError("Ping to OpenSearch failed. Check connectivity and credentials.") 

            # 修改日志信息
            logger.info("Successfully connected to OpenSearch.")
        except OSConnectionError as e: # 使用导入的 OSConnectionError
             logger.error(f"Failed to connect or ping OpenSearch: {e}")
             # raise
        except Exception as e:
            # 修改日志信息
            logger.error(f"An unexpected error occurred during OS client initialization: {e}")
            # raise

    # 将函数名注释改为 OpenSearch 相关
    def create_index(self):
        """创建OpenSearch索引（如果不存在）"""
        mapping = {
            'mappings': {
                'properties': {
                    '患者名': {'type': 'keyword'}, # 使用 keyword 类型以便精确匹配
                    '住院号': {'type': 'keyword'}, # 使用 keyword 类型以便精确匹配
                    '住院日期': {'type': 'date'},
                    '出院日期': {'type': 'date'},
                    '文件类型': {'type': 'keyword'},
                    '文件目录': {'type': 'keyword'},
                    '文件名称': {'type': 'keyword'}, # 使用 keyword 类型以便精确匹配
                    '页号': {'type': 'long'},
                    '页内容': {
                        'type': 'text',
                        'analyzer': 'ik_max_word',
                        'search_analyzer': 'ik_smart'
                    }
                }
            }
        }
        try:
            # 使用新的客户端变量操作索引
            if not self.os.indices.exists(index=self.index_name):
                self.os.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Created index: {self.index_name}")
            else:
                logger.info(f"Index {self.index_name} already exists")
        except RequestError as e: # RequestError 在 opensearch-py 中名称相同
            logger.error(f"Error creating index: {e}")
            raise

    # 将函数名注释改为 OpenSearch 相关
    def search(self, query, size=10):
        """搜索OpenSearch中的内容，返回页面信息"""
        search_body = {
            'query': {
                'match': {
                    '页内容': query
                }
            },
            'highlight': {
                'fields': {
                    '页内容': {
                        'pre_tags': ['<mark>'],
                        'post_tags': ['</mark>'],
                        'fragment_size': 200,
                        'number_of_fragments': 1
                    }
                }
            },
            '_source': ['文件名称', '页号', '页内容'],  # 仅返回必要字段
            'size': size
        }
        try:
            # 使用新的客户端变量进行搜索
            response = self.os.search(index=self.index_name, body=search_body)
            results = []
            for hit in response['hits']['hits']:
                # 注意：highlight 结果的结构在不同版本库中可能略有差异，
                # 但 opensearch-py 大部分与 elasticsearch-py 兼容
                highlight = hit.get('highlight', {}).get('页内容', [''])[0]
                results.append({
                    '文件名称': hit['_source']['文件名称'],
                    '页号': hit['_source']['页号'],
                    '页内容': hit['_source']['页内容'],
                    'score': hit['_score'],
                    'content_snippet': highlight or ''
                })
            return results
        except Exception as e:
            # 修改日志信息
            logger.error(f"Search error: {e}")
            return []


def main():
    # 将客户端类实例化改为新的类名
    config = SysConfig.load_config()["opensearch"]
    print(config)   
    os_client = OSClient(config)
    # 修改日志信息
    logger.info("Starting processing with OpenSearch")
    # 使用新的客户端变量调用方法
    os_client.create_index()
    # 如果需要搜索，这里可以调用 os_client.search()


if __name__ == "__main__":
    main()