# main_app.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import time
import datetime
import os
import logging
from logging.handlers import RotatingFileHandler # 用于日志文件输出
from sys_config import SysConfig
import queue
# 导入您之前编写的模块
try:
    from opensearch_client import OSClient
    from db_manager import IndexedFileManager
    from pdf_processor import PDFProcessor # 假设 PDFProcessor 包含了提取和索引逻辑
    from file_scanner import FileScanner
except ImportError as e:
    messagebox.showerror("导入错误", f"无法导入必要的模块：{e}\n请确保 opensearch_client.py, db_manager.py, pdf_processor.py, file_scanner.py 都在同一个目录下。")
    exit() # 如果导入失败，退出程序



# --- GUI 类 ---
class PDFIndexerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PDF 到 OpenSearch 索引工具")
        self.geometry("600x400")
        self.style = ttk.Style(self)
        self.setup_style()

        self.config = SysConfig.load_config()
        self.pdf_directory = tk.StringVar(value=self.config['app_settings'].get('pdf_directory', '')) # 使用 .get 防止 key 不存在
        self.scan_interval = self.config['app_settings'].get('scan_interval_seconds', 300)
        self.db_path = self.config['database'].get('db_path', 'indexed_files.db') # 使用 .get 防止 key 不存在

        self.scanning_thread = None
        self._is_scanning = False # 标志是否正在扫描

        # 确保初始化相关的变量
        self.os_client = None
        self.db_manager = None
        self.pdf_processor = None
        self.file_scanner = None
        self.message_queue = queue.Queue()  # 用于线程间通信的队列

        self.create_widgets()

        # 在程序关闭时保存配置
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

        # 配置日志
        self.setup_logging()
        # 测试日志
        self.logger = logging.getLogger(__name__)
        self.logger.info("Application initialized.")

        # 启动队列检查
        self.check_queue()

    def setup_logging(self):
        """配置日志系统，将输出重定向到 GUI、控制台和文件"""
        tk_handler = GUILogHandler(self)
        tk_handler.setLevel(logging.INFO)

        # 控制台的log
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        # 创建文件级别的log
        log_filename = f"app_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_filename, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(tk_handler)
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging to file: {os.path.abspath(log_filename)}")
    def check_queue(self):
        """多线程间发送GUIlog 定期检查队列并更新GUI"""
        try:
            while True:
                message = self.message_queue.get_nowait()
                self.append_log(f"[Thread Msg] {message}")
        except queue.Empty:
            pass
        # 保存 after ID，以便在退出时取消
        self._check_queue_after_id = self.after(100, self.check_queue)
    def append_log(self, message):
        try:
            self.log_text.config(state='normal')
            tag = 'normal'
            if '[ERROR]' in message:
                tag = 'error'
                self.log_text.tag_configure('error', foreground='red')
            self.log_text.insert(tk.END, message + '\n', tag)
            self.log_text.see(tk.END)
            self.log_text.config(state='disabled')
        except tk.TclError:
            pass
    
        
    def setup_style(self):
        """设置 ttk 样式"""
        self.style.theme_use('clam') # 或 'alt', 'default', 'classic'
        self.style.configure('TFrame', padding=5)
        self.style.configure('TLabel', padding=2)
        self.style.configure('TButton', padding=5)
        self.style.configure('TEntry', padding=5) # Entry 控件用于显示路径

    def create_widgets(self):
        """创建 GUI 控件"""
        # 目录选择 Frame
        dir_frame = ttk.Frame(self, padding="10")
        dir_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(dir_frame, text="PDF 目录:").pack(side=tk.LEFT)

        # 使用 Entry 显示路径并绑定 StringVar
        self.dir_entry = ttk.Entry(dir_frame, textvariable=self.pdf_directory, state='readonly', width=60) # state='readonly' 防止用户直接编辑
        self.dir_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        self.browse_button = ttk.Button(dir_frame, text="浏览", command=self.browse_directory)
        self.browse_button.pack(side=tk.LEFT)

        # 控制按钮 Frame
        control_frame = ttk.Frame(self, padding="10")
        control_frame.pack(fill=tk.X, padx=10, pady=5)

        self.start_stop_button = ttk.Button(control_frame, text="启动扫描", command=self.toggle_scan)
        self.start_stop_button.pack(side=tk.LEFT, expand=True) # 扩展按钮宽度

        # 状态/日志显示 Frame
        log_frame = ttk.Frame(self, padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        ttk.Label(log_frame, text="日志输出:").pack(side=tk.TOP, anchor=tk.W)

        # Text 控件用于显示日志，并添加滚动条
        self.log_text = tk.Text(log_frame, wrap=tk.WORD, state='disabled', height=10) # wrap=tk.WORD 按单词换行, state='disabled' 禁止编辑
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # 添加滚动条
        log_scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text['yscrollcommand'] = log_scrollbar.set


    def browse_directory(self):
        """打开目录选择对话框"""
        directory = filedialog.askdirectory(initialdir=self.pdf_directory.get() or ".") # 使用当前值或 '.' 作为初始目录
        if directory:
            self.pdf_directory.set(directory)
            self.config['app_settings']['pdf_directory'] = directory # 更新配置对象
            SysConfig.save_config(self.config) # 保存配置

    def toggle_scan(self):
        """启动或停止扫描进程"""
        if self._is_scanning:
            # 停止扫描
            self.stop_scan()
        else:
            # 启动扫描
            self.start_scan()

    def start_scan(self):
        """启动后台扫描线程"""
        pdf_dir = self.pdf_directory.get()
        if not os.path.isdir(pdf_dir):
            messagebox.showwarning("无效目录", "请选择一个有效的 PDF 存放目录。")
            self.logger.warning("Attempted to start scan with invalid directory.")
            return

        if self._is_scanning:
            self.logger.info("Scanning is already running.")
            return

        self.logger.info(f"Starting scan thread for directory: {pdf_dir}")
        self._is_scanning = True
        self.start_stop_button.config(text="停止扫描", state=tk.NORMAL) # 允许点击停止
        self.browse_button.config(state=tk.DISABLED) # 禁用浏览按钮

        # 创建并启动扫描线程
        # 注意：将初始化放在线程外可以避免每次扫描都重新创建客户端/数据库连接
        # 但需要确保这些对象是线程安全的，或者在线程内部创建它们。
        # 对于 SQLite 连接，如果使用 check_same_thread=False，同一个连接可以在不同线程使用，
        # 但更安全的做法是在线程内部获取连接或使用连接池。
        # 对于 OpenSearch 客户端，opensearch-py 客户端对象通常是线程安全的。
        # 这里我们在启动线程前初始化，并在 on_closing 中清理。

        try:
            # 初始化 OpenSearch 客户端, DB 管理器, PDF 处理器, 文件扫描器
            # OSClient 的 config_path 参数可能需要调整，或者您在 config.json 中包含所有配置
            # 简单起见，我们假设 OSClient 能从 config.json 找到配置，或者您修改其 __init__
            # 这里假设 OSClient 构造函数读取 config.json 中的 opensearch 部分
            # 或者您可以直接传递配置字典
            opensearch_config = self.config.get('opensearch', {})
            db_config = self.config.get('database', {})

            # 在这里初始化，确保它们能被线程访问
            self.os_client = OSClient(opensearch_config) # 或者传递配置字典
            self.db_manager = IndexedFileManager(db_path=db_config.get('db_path', 'indexed_files.db'))

            # PDFProcessor 需要 OSClient 实例
            self.pdf_processor = PDFProcessor(self.os_client)

            # FileScanner 需要 DBManager 和 PDFProcessor 实例
            self.file_scanner = FileScanner(self.db_manager, self.pdf_processor)

            # 启动扫描线程
            self.scanning_thread = threading.Thread(target=self._run_scan_loop, args=(pdf_dir,))
            self.scanning_thread.daemon = True # 设置为守护线程，主程序退出时自动退出
            self.scanning_thread.start()

        except Exception as e:
            self.logger.error(f"Failed to initialize components or start scan thread: {e}")
            self.stop_scan() # 发生错误时重置按钮状态等

    def stop_scan(self):
        """停止后台扫描线程"""
        if not self._is_scanning:
            self.logger.info("Scanning is not currently running.")
            return

        self.logger.info("Stopping scan thread...")
        self._is_scanning = False # 设置标志
        if self.file_scanner:
             self.file_scanner.stop_scanning() # 调用 FileScanner 的停止方法

        # GUI 状态更新
        self.start_stop_button.config(text="启动扫描")
        self.browse_button.config(state=tk.NORMAL) # 重新启用浏览按钮

        # 可以选择在这里等待线程结束，但这会阻塞 GUI
        # 如果设置为 daemon 线程，通常不需要显式join，退出主程序线程即可

    def _run_scan_loop(self, directory_path):
        """后台线程中运行的扫描循环"""
        self.logger.info(f"Scan loop thread started for directory: {directory_path}")
        while self._is_scanning:
            try:
                # 在每次扫描开始前检查停止标志
                if not self._is_scanning:
                    break

                # --- 调用文件扫描和索引的主逻辑 ---
                # 这个方法在 file_scanner.py 中实现
                # 它会遍历目录，检查数据库，调用 pdf_processor 进行提取和索引
                if self.file_scanner:
                    self.file_scanner.scan_and_index_directory(directory_path)
                else:
                    self.logger.error("FileScanner is not initialized in the scanning thread.")
                    break # 无法扫描，退出循环


            except Exception as e:
                self.logger.error(f"Error during scan loop iteration: {e}")
                # 在发生错误时暂停一段时间，避免无限循环错误
                time.sleep(60) # 例如，错误后暂停 60 秒再尝试下一次循环

            # 在每次扫描周期结束后暂停
            if self._is_scanning: # 再次检查标志，确保在扫描完成后到暂停期间用户没有点击停止
                self.logger.info(f"Scan loop finished one cycle. Waiting {self.scan_interval} seconds for next scan.")
                time.sleep(self.scan_interval)

        self.logger.info("Scan loop thread finished.")


    def on_closing(self):
        """处理窗口关闭事件"""
        if messagebox.askokcancel("退出", "确定要退出程序吗？"):
            
            self.logger.info("Application is shutting down.")
                # 取消 check_queue 的 after 调度
            if self._check_queue_after_id is not None:
                try:
                    self.after_cancel(self._check_queue_after_id)
                    self.logger.info("Cancelled check_queue after scheduling.")
                except tk.TclError:
                    self.logger.warning("Failed to cancel check_queue after scheduling, possibly already destroyed.")
                self._check_queue_after_id = None

            self.stop_scan() # 尝试停止扫描线程
            # 在这里可以添加清理代码，如关闭数据库连接等
            # 对于 daemon 线程，通常不需要显式清理，它们会随主线程退出
            # 但如果是非 daemon 线程或需要确保资源释放，可以在这里等待线程结束
            # if self.scanning_thread and self.scanning_thread.is_alive():
            #     self.scanning_thread.join(timeout=5) # 等待线程最多5秒

            SysConfig.save_config(self.config) # 确保在退出时保存最新的目录设置
            self.destroy() # 销毁窗口




class GUILogHandler(logging.Handler):
    """自定义 Logging Handler，将日志输出到 Tkinter 文本框"""
    def __init__(self, app):
        super().__init__()
        self.app = app  # SimpleThreadTestApp 实例
        # 设置日志格式
        self.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

    def emit(self, record):
        """处理日志记录，将格式化的消息放入队列"""
        try:
            msg = self.format(record)  # 格式化日志消息
            self.app.message_queue.put(('log', msg))  # 放入队列，标记为日志消息
        except Exception:
            self.handleError(record)



# --- 主程序入口 ---
if __name__ == "__main__":
    app = PDFIndexerApp()
    app.mainloop() # 启动 Tkinter 事件循环



#基于命令行的最简单的测试
###################################################################
#  python -c "from main import TestOpenSearch; TestOpenSearch()"
###################################################################

def TestOpenSearch():
    config = SysConfig.load_config().get("opeansearch")
    #连接opensearch
    os_client = OSClient(config)
    #创建索引
    os_client.create_index()

    # 配置
    PDF_DIRECTORY = './pdf_files'  # 替换为你的PDF文件目录
    # 索引目录中的所有PDF文件，把PDF内容提取出来，并保存到OpenSearch中
    pdf_processor = PDFProcessor(os)
    pdf_processor.index_directory(PDF_DIRECTORY)

    # 如果需要搜索，这里可以调用 os_client.search()

