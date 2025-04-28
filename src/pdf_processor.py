import os
import logging
from search_model import SearchModel
from opensearch_client import OSClient
from opensearchpy import OpenSearch, helpers # <-- 导入 helpers
from io import StringIO
from pdfminer.layout import LAParams

# 设置日志
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFProcessor:
    """处理PDF文件的类，提取文本内容并获取文件元数据"""
    def __init__(self, os_client):
        self.os_client = os_client
    def extract_text_with_pdfminer_six(self,pdf_path):
        pages_text = []
        try:
            # 创建一个 StringIO 对象来捕获提取的文本
            output_string = StringIO()

            # 配置 LAParams 对象，可以调整参数来优化文本布局解析
            # 例如：all_texts=True 可以尝试提取所有文本对象，包括被遮挡的
            # detect_vertical=True 可以帮助处理垂直文本
            laparams = LAParams()

            # 使用 extract_text_to_fp 提取文本到 StringIO 对象
            # 可以指定 page_numbers 参数来只提取特定页
            # 这里我们遍历每一页进行提取以保留页号信息
            # 注意：pdfminer.six 的 extract_text_to_fp 默认是处理整个文件的
            # 为了按页提取，我们需要更底层的控制，或者外部循环处理

            # 为了按页处理并获取页号，我们使用一个更适合按页提取的方法
            # 这通常涉及遍历 PDFPage 对象

            from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
            from pdfminer.converter import TextConverter
            from pdfminer.pdfpage import PDFPage

            resource_manager = PDFResourceManager()

            with open(pdf_path, 'rb') as fp:
                for page_num, page in enumerate(PDFPage.get_pages(fp, caching=True, check_extractable=True), start=1):
                    output_string = StringIO()
                    device = TextConverter(resource_manager, output_string, laparams=laparams)
                    interpreter = PDFPageInterpreter(resource_manager, device)

                    try:
                        interpreter.process_page(page)
                        text = output_string.getvalue()

                        if text and text.strip():
                            pages_text.append({
                                '页号': page_num,
                                '页内容': text.strip()
                            })
                    except Exception as page_e:
                        logger.error(f"Error extracting text from page {page_num} of {pdf_path} with pdfminer.six: {page_e}")

                    device.close()
                    output_string.close() # 关闭 StringIO 对象以释放资源

            return pages_text

        except FileNotFoundError:
            logger.error(f"PDF file not found: {pdf_path}")
            return []
        except Exception as e:
            logger.error(f"Error processing PDF file {pdf_path} with pdfminer.six: {e}")
            return []
        
    def index_pdf(self, pdf_path):
        """将PDF文件的每页内容批量索引到Elasticsearch"""
        logger.info(f"Indexing PDF: {pdf_path}")
        esmodel = SearchModel()
        esmodel.parse_fname(pdf_path,os.path.basename(pdf_path))
        pages = self.extract_text_with_pdfminer_six(pdf_path)
        if not pages:
            logger.error(f"Failed to extract content from PDF: {pdf_path}")
            return False

        documents_for_bulk = []
        for page in pages:
            # 1. 获取实例的变量名称和值
            # vars(model_instance) 返回一个字典，键是属性名，值是属性值
            document_data = vars(esmodel)

            # 移除我们不希望作为文档字段直接存储的属性，例如 doc_id，因为它用作 Elasticsearch 的 _id
            # 创建一个新的字典，不包含 doc_id
            source_data = {key: value for key, value in document_data.items() if key != '页号' or key != '页内容'}
            source_data['页号'] = page['页号']
            source_data['页内容'] = page['页内容']

            # 2. 构建符合 Elasticsearch 批量索引格式的字典
            bulk_item = {
                "_index": self.os_client.index_name, # 使用实例的索引名称
                "_source": source_data # 将变量名称和值构成的字典作为文档源数据
                # 如果你想确保是创建新文档而不是更新，可以添加 "_op_type": "create"
                # "_op_type": "create"
            }
            documents_for_bulk.append(bulk_item)

        try:
            # bulk(self.es, actions)
            #opensearchpy.helpers.bulk(self.es, documents_for_bulk)
            success_count, errors = helpers.bulk(self.os_client.os, documents_for_bulk)
            if errors:
                logger.error(f"Bulk indexing for {pdf_path} finished with errors. Success count: {success_count}")
                # 您可能需要进一步检查 errors 列表以查看具体哪些文档索引失败了
                # logger.error(f"Bulk errors: {errors}") # 注意：errors 可能很大，谨慎打印
            else:
                logger.info(f"Successfully bulk indexed {success_count} documents from {pdf_path}")

            return True
        except Exception as e:
            logger.error(f"Error bulk indexing {pdf_path}: {e}")
            return False
    def index_directory(self, directory):
        """索引指定目录中的所有PDF文件"""
        if not os.path.isdir(directory):
            logger.error(f"Directory {directory} does not exist")
            return

        for root, _, files in os.walk(directory):
            for file in files:
                if file.lower().endswith('.pdf'):
                    pdf_path = os.path.join(root, file)
                    self.index_pdf(pdf_path)


def main():
    # 配置
    PDF_DIRECTORY = './pdf_files'  # 替换为你的PDF文件目录

    # 初始化索引器
    from sys_config import SysConfig
    config = SysConfig.load_config().get("opeansearch")
    indexer = OSClient(config)
    indexer.create_index()

    # 索引目录中的所有PDF文件
    pdf_processor = PDFProcessor(indexer)
    pdf_processor.index_directory(PDF_DIRECTORY)
    
    # 示例搜索
    query = "住院"  # 替换为你的搜索关键词
    logger.info(f"Searching for: {query}")
    results = indexer.search(query)
    
    # 打印搜索结果
    for result in results:
        print(f"\nFile: {result['文件名称']}")
        print(f"Path: {result['文件目录']}")
        print(f"Page: {result['页号']}")
        print(f"Score: {result['score']}")
        print(f"Content Snippet: {result['content_snippet']}")

if __name__ == '__main__':
    main()