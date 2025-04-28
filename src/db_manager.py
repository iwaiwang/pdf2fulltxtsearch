# db_manager.py
import sqlite3
import os
import logging
import time

logger = logging.getLogger(__name__)

class IndexedFileManager:
    def __init__(self, db_path="indexed_files.db"):
        self.db_path = db_path
        self._create_table()

    def _get_connection(self):
        """获取数据库连接"""
        # check_same_thread=False 是为了支持多线程访问，但在多线程环境下最好为每个线程创建独立的连接
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _create_table(self):
        """创建存储已索引文件信息的表"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS indexed_files (
                    file_path TEXT PRIMARY KEY,
                    modification_time REAL, -- 文件最后修改时间戳 (浮点数)
                    indexed_time REAL -- 索引到 OpenSearch 的时间戳
                )
            ''')
            # 为 file_path 字段添加索引
            cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_file_path ON indexed_files (file_path)
            ''')
            conn.commit()
            logger.info(f"SQLite database table 'indexed_files' checked/created at {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error creating database table: {e}")
        finally:
            if conn:
                conn.close()

    def is_indexed(self, file_path, modification_time):
        """
        检查文件是否已索引，并且自上次索引后未被修改。

        Args:
            file_path (str): 文件的完整路径。
            modification_time (float): 文件的最后修改时间戳。

        Returns:
            bool: 如果文件已索引且未修改，则返回 True；否则返回 False (表示需要索引)。
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT modification_time FROM indexed_files WHERE file_path = ?", (file_path,))
            row = cursor.fetchone()

            if row is None:
                # 文件未找到，需要索引
                return False
            else:
                # 文件找到，检查修改时间戳
                stored_mod_time = row[0]
                # 考虑到浮点数比较可能不精确，可以设置一个小的容忍度，
                # 或者直接比较是否不等于。os.path.getmtime 返回的是精确浮点数。
                # 简单起见，我们直接比较不等于
                if modification_time > stored_mod_time:
                    # 文件已被修改，需要重新索引
                    logger.info(f"File modified: {file_path}. Needs re-indexing.")
                    return False
                else:
                    # 文件已索引且未修改
                    # logger.debug(f"File already indexed and not modified: {file_path}")
                    return True
        except sqlite3.Error as e:
            logger.error(f"Error checking if file is indexed: {file_path} - {e}")
            # 发生数据库错误时，谨慎起见返回 False，让文件有机会被重新处理
            return False
        finally:
            if conn:
                conn.close()

    def mark_as_indexed(self, file_path, modification_time):
        """
        标记文件为已索引，存储其路径和修改时间戳。
        如果文件已存在，则更新其信息（用于重新索引后更新时间戳）。

        Args:
            file_path (str): 文件的完整路径。
            modification_time (float): 文件的最后修改时间戳。
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            indexed_time = os.path.getmtime(self.db_path) # 使用数据库文件的修改时间或当前时间
            # 使用 INSERT OR REPLACE 来处理插入或更新
            cursor.execute(
                "INSERT OR REPLACE INTO indexed_files (file_path, modification_time, indexed_time) VALUES (?, ?, ?)",
                (file_path, modification_time, time.time()) # 记录当前系统时间作为索引时间
            )
            conn.commit()
            logger.info(f"Marked file as indexed: {file_path}")
        except sqlite3.Error as e:
            logger.error(f"Error marking file as indexed: {file_path} - {e}")
        finally:
            if conn:
                conn.close()

    def remove_indexed_record(self, file_path):
        """
        从数据库中移除文件的索引记录。

        Args:
            file_path (str): 文件的完整路径。
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM indexed_files WHERE file_path = ?", (file_path,))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(f"Removed indexed record for: {file_path}")
            # else:
                # logger.debug(f"No indexed record found for removal: {file_path}")
        except sqlite3.Error as e:
            logger.error(f"Error removing indexed record: {file_path} - {e}")
        finally:
            if conn:
                conn.close()

    def get_all_indexed_files(self):
        """
        获取数据库中所有已索引文件的路径列表。

        Returns:
            list: 所有已索引文件的文件路径列表。
        """
        conn = None
        indexed_files = []
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT file_path FROM indexed_files")
            rows = cursor.fetchall()
            indexed_files = [row[0] for row in rows]
            logger.info(f"Retrieved {len(indexed_files)} indexed file paths from DB.")
        except sqlite3.Error as e:
            logger.error(f"Error retrieving all indexed files: {e}")
        finally:
            if conn:
                conn.close()
        return indexed_files
