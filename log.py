"""
log.py

统一的日志配置和工具模块

功能:
- 提供统一的日志配置
- 支持彩色日志输出（可配置）
- 提供便捷的日志记录函数
- 支持文件和控制台双重输出
- 兼容不同操作系统的终端

设计原则:
- 统一的日志格式和配置
- 可配置的彩色输出
- 线程安全的日志记录
- 简洁的API接口
"""

import logging
import sys
import os
from typing import Optional
from logging.handlers import RotatingFileHandler


class Colors:
    """ANSI颜色代码"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


class ColoredFormatter(logging.Formatter):
    """支持彩色输出的日志格式化器"""

    def __init__(self, fmt: str, use_colors: bool = True):
        super().__init__(fmt)
        self.use_colors = use_colors and self._supports_color()

        # 定义不同日志级别的颜色
        self.colors = {
            logging.DEBUG: Colors.CYAN,
            logging.INFO: Colors.WHITE,
            logging.WARNING: Colors.YELLOW,
            logging.ERROR: Colors.RED,
            logging.CRITICAL: Colors.MAGENTA + Colors.BOLD,
        }

    def _supports_color(self) -> bool:
        """检查终端是否支持彩色输出"""
        # 检查是否在支持彩色的终端中
        if not hasattr(sys.stdout, 'isatty') or not sys.stdout.isatty():
            return False

        # Windows系统需要特殊处理
        if os.name == 'nt':
            try:
                import colorama
                colorama.init()
                return True
            except ImportError:
                return False

        # Unix系统通常支持彩色
        return True

    def format(self, record):
        if self.use_colors and record.levelno in self.colors:
            # 为整个消息添加颜色
            color = self.colors[record.levelno]
            record.msg = f"{color}{record.msg}{Colors.RESET}"

        return super().format(record)


class LogManager:
    """日志管理器"""

    def __init__(self):
        self.logger = None
        self._initialized = False

    def setup_logging(self,
                      level: int = logging.INFO,
                      log_file: Optional[str] = None,
                      use_colors: bool = True,
                      max_file_size: int = 10 * 1024 * 1024,  # 10MB
                      backup_count: int = 5) -> logging.Logger:
        """
        设置统一的日志配置
        
        Args:
            level: 日志级别
            log_file: 日志文件路径，None表示不输出到文件
            use_colors: 是否使用彩色输出
            max_file_size: 日志文件最大大小（字节）
            backup_count: 日志文件备份数量
            
        Returns:
            配置好的logger对象
        """
        if self._initialized:
            return self.logger

        # 创建根logger
        self.logger = logging.getLogger('excel_operator')
        self.logger.setLevel(level)

        # 清除已有的处理器
        self.logger.handlers.clear()

        # 日志格式
        log_format = '[%(asctime)s] %(levelname)s: %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_formatter = ColoredFormatter(log_format, use_colors)
        console_formatter.datefmt = date_format
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        # 文件处理器（如果指定了日志文件）
        if log_file:
            try:
                # 确保日志目录存在
                log_dir = os.path.dirname(log_file)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir)

                file_handler = RotatingFileHandler(
                    log_file,
                    maxBytes=max_file_size,
                    backupCount=backup_count,
                    encoding='utf-8'
                )
                file_handler.setLevel(level)

                # 文件输出不使用颜色
                file_formatter = logging.Formatter(log_format, date_format)
                file_handler.setFormatter(file_formatter)
                self.logger.addHandler(file_handler)

                self.logger.info(f"日志文件已配置: {log_file}")
            except Exception as e:
                self.logger.warning(f"无法配置日志文件 {log_file}: {e}")

        # 防止日志传播到根logger
        self.logger.propagate = False

        self._initialized = True
        return self.logger

    def get_logger(self) -> logging.Logger:
        """获取logger实例"""
        if not self._initialized:
            return self.setup_logging()
        return self.logger


# 全局日志管理器实例
_log_manager = LogManager()


def setup_logging(level: int = logging.INFO,
                  log_file: Optional[str] = None,
                  use_colors: bool = True) -> logging.Logger:
    """
    设置统一的日志配置（便捷函数）
    
    Args:
        level: 日志级别
        log_file: 日志文件路径
        use_colors: 是否使用彩色输出
        
    Returns:
        配置好的logger对象
    """
    return _log_manager.setup_logging(level, log_file, use_colors)


def get_logger() -> logging.Logger:
    """获取logger实例（便捷函数）"""
    return _log_manager.get_logger()


# 便捷的日志记录函数
def log_success(message: str, logger: Optional[logging.Logger] = None):
    """输出成功日志"""
    if logger is None:
        logger = get_logger()
    logger.info(f"✅ {message}")


def log_error(message: str, logger: Optional[logging.Logger] = None):
    """输出错误日志"""
    if logger is None:
        logger = get_logger()
    logger.error(f"❌ {message}")


def log_info(message: str, logger: Optional[logging.Logger] = None):
    """输出信息日志"""
    if logger is None:
        logger = get_logger()
    logger.info(f"ℹ️  {message}")


def log_warning(message: str, logger: Optional[logging.Logger] = None):
    """输出警告日志"""
    if logger is None:
        logger = get_logger()
    logger.warning(f"⚠️  {message}")


def log_timing(operation: str, execution_time: float, logger: Optional[logging.Logger] = None):
    """输出执行时间日志"""
    if logger is None:
        logger = get_logger()
    logger.info(f"⏱️  {operation} (耗时: {execution_time:.3f}s)")


def log_stage(stage: str, message: str, logger: Optional[logging.Logger] = None):
    """输出阶段性日志"""
    if logger is None:
        logger = get_logger()
    logger.info(f"🔄 [{stage}] {message}")


def log_progress(current: int, total: int, message: str = "", logger: Optional[logging.Logger] = None):
    """输出进度日志"""
    if logger is None:
        logger = get_logger()
    percentage = (current / total * 100) if total > 0 else 0
    progress_msg = f"📊 进度: {current}/{total} ({percentage:.1f}%)"
    if message:
        progress_msg += f" - {message}"
    logger.info(progress_msg)


# 兼容性函数（保持向后兼容）
def execute_sql_with_timing(conn, sql: str, operation_name: str, logger: Optional[logging.Logger] = None):
    """
    执行SQL并记录执行时间的辅助函数
    
    Args:
        conn: DuckDB连接对象
        sql: 要执行的SQL语句
        operation_name: 操作名称，用于日志显示
        logger: 日志记录器
        
    Returns:
        SQL执行结果
        
    Raises:
        Exception: SQL执行失败时抛出异常
    """
    import time

    if logger is None:
        logger = get_logger()

    try:
        start_time = time.time()
        result = conn.execute(sql)
        execution_time = time.time() - start_time
        log_timing(operation_name, execution_time, logger)
        return result
    except Exception as e:
        execution_time = time.time() - start_time
        log_error(f"{operation_name} 执行失败 (耗时: {execution_time:.3f}s): {str(e)}", logger)
        raise


if __name__ == "__main__":
    # 测试日志功能
    logger = setup_logging(log_file="test.log")

    log_info("这是一条信息日志")
    log_success("这是一条成功日志")
    log_warning("这是一条警告日志")
    log_error("这是一条错误日志")
    log_stage("测试阶段", "正在执行测试操作")
    log_progress(3, 10, "处理文件")
    log_timing("测试操作", 1.234)

    print("日志测试完成，请检查控制台输出和test.log文件")
