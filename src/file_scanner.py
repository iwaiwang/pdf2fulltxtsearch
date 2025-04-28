import os
import logging
from db_manager import IndexedFileManager



logger = logging.getLogger(__name__)

class FileScanner:
    def __init__(self, db_manager: IndexedFileManager, pdf_processor): # Pass the PDFProcessor instance
        self.db_manager = db_manager
        self.pdf_processor = pdf_processor # Needs an instance of PDFProcessor
        self._stop_scanning = False

    def scan_and_index_directory(self, directory_path):
        """
        扫描指定目录下的所有 PDF 文件，检查其索引状态并进行索引。

        Args:
            directory_path (str): 要扫描的根目录路径。
        """
        if not os.path.isdir(directory_path):
            logger.error(f"Scan directory does not exist: {directory_path}")
            return

        logger.info(f"Starting scan and index for directory: {directory_path}")
        processed_count = 0
        skipped_count = 0
        error_count = 0

        # TODO: Add logic here to handle potential deletion of files from DB/OpenSearch
        # based on files that are in the DB but not found on the filesystem.
        # This requires getting all files from DB first, then iterating files on disk,
        # and finally checking which DB files were not encountered.

        for root, _, files in os.walk(directory_path):
            if self._stop_scanning:
                logger.info("Scanning stopped by user request.")
                break

            for file in files:
                if self._stop_scanning:
                     logger.info("Scanning stopped by user request.")
                     break

                if file.lower().endswith('.pdf'):
                    pdf_path = os.path.join(root, file)

                    try:
                        # 获取文件的最后修改时间
                        modification_time = os.path.getmtime(pdf_path)

                        # 检查文件是否需要索引（新文件或修改文件）
                        if not self.db_manager.is_indexed(pdf_path, modification_time):
                            logger.info(f"Processing file: {pdf_path}")

                            # --- 在这里调用 PDF 文本提取和 OpenSearch 索引逻辑 ---
                            # 这部分应该由传入的 pdf_processor 实例来完成
                            # pdf_processor.index_pdf(pdf_path) 方法应该包含提取和bulk索引的逻辑
                            # 您可能需要从文件名解析患者信息等，这取决于您的 SearchDocument 结构

                            # 示例调用 (假设 pdf_processor 有 index_pdf 方法)
                            # 您可能需要根据实际情况调整 index_pdf 的参数
                            # 例如，如果 patient_info 需要从文件名解析，可以在这里处理
                            # patient_info = self._parse_patient_info_from_path(pdf_path) # 实现这个方法
                            # success = self.pdf_processor.index_pdf(pdf_path, patient_info) # 传递patient_info

                            # 简化的调用，假设 index_pdf 自己处理从文件名获取 info 或只需要 path
                            success = self.pdf_processor.index_pdf(pdf_path)
                            if success:
                                # 索引成功后，标记文件为已索引
                                self.db_manager.mark_as_indexed(pdf_path, modification_time)
                                processed_count += 1
                            else:
                                logger.error(f"Failed to index file: {pdf_path}")
                                error_count += 1
                        else:
                            # 文件已索引且未修改，跳过
                            # logger.debug(f"Skipping already indexed file: {pdf_path}")
                            skipped_count += 1

                    except FileNotFoundError:
                        # 文件在扫描后但在处理前被删除，跳过
                        logger.warning(f"File not found during processing (might have been deleted): {pdf_path}")
                        # 可以选择从数据库中移除此记录 if needed
                        # self.db_manager.remove_indexed_record(pdf_path)
                        error_count += 1 # 视为处理过程中的错误/异常情况
                    except Exception as e:
                        # 处理文件时发生其他错误（如PDF解析错误，OpenSearch连接错误等）
                        logger.error(f"Error processing file {pdf_path}: {e}")
                        error_count += 1
                        # 这里的错误处理取决于需求，是否重试、记录失败日志等


        logger.info(f"Scan and index finished for directory: {directory_path}")
        logger.info(f"Processed: {processed_count}, Skipped (already indexed): {skipped_count}, Errors: {error_count}")


    def stop_scanning(self):
        """设置标志以停止正在进行的扫描"""
        self._stop_scanning = True
        logger.info("Stop scanning requested.")

    