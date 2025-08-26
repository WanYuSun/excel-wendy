"""
excel_operator.py

一个用于在子目录中批量处理Excel文件并用DuckDB分析的跨平台工具。

使用说明:
1. 程序执行路径要求
   - 请在包含若干子目录的主目录下运行本程序。
   - 每个子目录作为一个处理入口，子目录名称即为入口名。
   - 推荐将本脚本放在这些子目录的上一级目录下执行。

2. 预期输入
   - 每个子目录下应包含若干Excel文件（.xlsx）。
   - "广点通"入口要求子目录下有：
     - 主数据文件，文件名格式如：广点通1234xxxx.xlsx（数字开头）
     - 账户文件，文件名格式如：xxxx账号列表.xlsx
   - "快手"入口要求子目录下有：
     - 消耗文件，文件名格式如：快手消耗1234xxxx.xlsx（数字开头）
     - 充值文件，文件名格式如：快手充值1234xxxx.xlsx（数字开头）
   - "头条"入口要求子目录下有：
     - 消耗文件，文件名格式如：头条消耗_\d+xxxx.xlsx
     - 充值文件，文件名格式如：头条充值_\d+xxxx.xlsx
     - 共享钱包流水文件，文件名格式如：头条共享钱包流水下载_\d+xxxx.xlsx
   - 若缺少上述任一文件，该子目录将跳过处理，并在日志中提示。

3. 输出位置
   - 处理结果输出为Excel文件，命名格式为 output_{子目录名}.xlsx。
   - 若同名文件已存在，则为 output_{子目录名}_2.xlsx、output_{子目录名}_3.xlsx 等。
   - 输出文件统一保存在各子目录的上一级目录（即主目录）下。

功能:
- 列出当前工作目录下所有子目录，每个子目录作为一个处理入口。
- 对每个子目录，收集所有`.xlsx`文件。
- 提供入口处理接口，可自定义处理逻辑。
- 支持周结和月结两种处理模式
- 全过程中文日志提示。
- 兼容Windows和Linux。

依赖:
- Python 3.7+
- duckdb (`pip install duckdb`)

可选: 打包为可执行文件
    python -m pip install pyinstaller
    pyinstaller --onefile excel_operator.py

用法:
    python excel_operator.py

"""

import multiprocessing
import os
import sys
from typing import List, Callable, Dict

# 确保项目根目录在 Python 模块搜索路径中
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import duckdb

# 导入周结处理器
from excel.handlers.week.guangdiantong import guangdiantong_entry_handler as guangdiantong_week_handler
from excel.handlers.week.guangdiantong_v2 import guangdiantong_v2_entry_handler as guangdiantong_v2_week_handler
from excel.handlers.week.kuaishou import kuaishou_entry_handler as kuaishou_week_handler
from excel.handlers.week.toutiao import toutiao_entry_handler as toutiao_week_handler

# 导入月结处理器
from excel.handlers.month.guangdiantong import guangdiantong_month_entry_handler
from excel.handlers.month.guangdiantong_v2 import guangdiantong_v2_month_entry_handler
from excel.handlers.month.kuaishou import kuaishou_month_entry_handler
from excel.handlers.month.toutiao import toutiao_month_entry_handler

# 导入统一的日志模块
from excel.log import (setup_logging, log_success, log_error, log_info,
                       log_warning, log_stage, log_progress, execute_sql_with_timing)

# 设置统一的日志配置
logger = setup_logging(log_file="excel_operator.log")

# 导入本地select模块
try:
    from excel.select_excels import select_from_excel

    log_success("成功导入select模块")
except ImportError as e:
    log_error(f"导入select模块失败: {e}")
    sys.exit(1)

# 导入union-sheets.py中的函数（用于account表加载）
try:
    from excel.union_sheets import union_sheets_concurrent, unique_keys

    log_success("成功导入union-sheets模块")
except ImportError as e:
    log_error(f"导入union-sheets模块失败: {e}")
    sys.exit(1)


def list_process_entries(base_dir: str) -> List[str]:
    """
    列出base_dir下所有子目录，作为处理入口。
    """
    log_stage("扫描目录", f"正在扫描目录: {base_dir}")
    entries = [
        os.path.join(base_dir, d)
        for d in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, d))
    ]
    log_info(f"在目录 {base_dir} 下发现 {len(entries)} 个处理入口（子目录）")
    return entries


def list_excels(entry_dir: str) -> List[str]:
    """
    列出指定目录下所有xlsx文件，返回相对文件名列表。
    """
    log_stage("文件扫描", f"正在扫描Excel文件: {os.path.basename(entry_dir)}")
    excels = [
        f for f in os.listdir(entry_dir)
        if (f.lower().endswith('.xlsx') and
            os.path.isfile(os.path.join(entry_dir, f)))
    ]
    log_info(f"  {os.path.basename(entry_dir)}: 发现 {len(excels)} 个xlsx文件")
    return excels


# 入口处理函数类型定义
EntryHandler = Callable[[str, List[str], duckdb.DuckDBPyConnection], None]
week_entry_registry: Dict[str, EntryHandler] = {}
month_entry_registry: Dict[str, EntryHandler] = {}


def register_week_entry(name: str, handler: EntryHandler):
    """
    注册周结入口处理函数
    """
    week_entry_registry[name] = handler


def register_month_entry(name: str, handler: EntryHandler):
    """
    注册月结入口处理函数
    """
    month_entry_registry[name] = handler


def handle_entry(entry_name: str, entry_dir: str, excels: List[str],
                 conn: duckdb.DuckDBPyConnection, processing_type: str):
    """
    调用指定入口名的处理函数
    支持entry_name形式：entry 或 entry-subentry
    使用startswith匹配handler_key
    
    Args:
        entry_name: 入口名称
        entry_dir: 入口目录路径
        excels: Excel文件列表
        conn: DuckDB连接对象
        processing_type: 处理类型 ('week' 或 'month')
    """
    log_stage("入口处理", f"开始处理入口: {entry_name} (类型: {processing_type})")

    # 根据处理类型选择注册表
    entry_registry = week_entry_registry if processing_type == 'week' else month_entry_registry
    
    handler = None
    matched_key = None

    # 首先尝试精确匹配
    if entry_name in entry_registry:
        handler = entry_registry[entry_name]
        matched_key = entry_name
    else:
        # 如果精确匹配失败，尝试startswith匹配
        for handler_key in entry_registry:
            if entry_name.startswith(handler_key):
                handler = entry_registry[handler_key]
                matched_key = handler_key
                break

    if not handler:
        log_warning(f"未注册{processing_type}入口处理器: '{entry_name}'")
        return

    log_info(f"[{entry_name}] 使用{processing_type}处理器: {matched_key}")

    try:
        handler(entry_dir, excels, conn)
        log_success(f"[{entry_name}] {processing_type}入口处理完成")
    except Exception as e:
        log_error(f"[{entry_name}] {processing_type}入口处理失败: {e}")
        raise


# 注册周结处理器
register_week_entry("广点通", guangdiantong_week_handler)
register_week_entry("广点通-大端口", guangdiantong_v2_week_handler)
register_week_entry("快手", kuaishou_week_handler)
register_week_entry("头条", toutiao_week_handler)

# 注册月结处理器
register_month_entry("广点通", guangdiantong_month_entry_handler)
register_month_entry("广点通-大端口", guangdiantong_v2_month_entry_handler)
register_month_entry("快手", kuaishou_month_entry_handler)
register_month_entry("头条", toutiao_month_entry_handler)


def select_processing_type() -> str:
    """
    让用户选择处理类型：周结或月结
    
    Returns:
        str: 'week' 或 'month'
    """
    log_stage("处理类型选择", "请选择数据处理类型")
    
    while True:
        print("\n请选择数据处理类型:")
        print("1. 周结数据处理 (数据量较小，处理速度快)")
        print("2. 月结数据处理 (数据量较大，使用并发优化)")
        print("3. 退出程序")
        
        choice = input("\n请输入选项编号 (1/2/3): ").strip()
        
        if choice == '1':
            log_success("选择周结数据处理模式")
            return 'week'
        elif choice == '2':
            log_success("选择月结数据处理模式")
            return 'month'
        elif choice == '3':
            log_info("用户选择退出程序")
            return 'exit'
        else:
            print("无效选择，请输入 1、2 或 3")


def load_account_table(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    加载account表的函数

    Args:
        conn: DuckDB连接对象

    Returns:
        bool: 是否成功加载account表
    """
    log_stage("账户表加载", "准备加载媒体账户表")

    # 询问用户是否加载account表
    while True:
        choice = input("是否加载媒体账户表(account)？(y/n): ").strip().lower()
        if choice in ['y', 'yes', '是']:
            break
        elif choice in ['n', 'no', '否']:
            log_info("跳过加载account表")
            return False
        else:
            print("请输入 y/yes/是 或 n/no/否")

    # 提示用户输入媒体账户Excel文件的绝对路径
    log_stage("文件选择", "选择媒体账户Excel文件")
    while True:
        excel_file = input("请输入媒体账户Excel文件的绝对路径: ").strip().strip('"\'')
        if (os.path.exists(excel_file) and
                excel_file.lower().endswith('.xlsx')):
            log_success(f"选择文件: {os.path.basename(excel_file)}")
            break
        else:
            print("文件不存在或不是Excel文件，请重新输入")

    try:
        log_stage("数据加载", f"开始加载account表，Excel文件: {excel_file}")

        # 预先加载Excel扩展
        log_info("预加载Excel扩展")
        try:
            execute_sql_with_timing(conn, "INSTALL excel", "安装Excel扩展")
            execute_sql_with_timing(conn, "LOAD excel", "加载Excel扩展")
            log_success("Excel扩展预加载成功")
        except Exception as e:
            log_info(f"Excel扩展已存在: {e}")

        # 使用union_sheets_concurrent加载数据到u_account表
        log_stage("数据合并", "合并Excel多个sheets到临时表")
        projections = [
            ('"账号ID"', "id"),
            ('"媒体账户主体"', "n1"),
            ('"客户名称"', "n2")
        ]

        union_sheets_concurrent(
            excel_file=excel_file,
            table_name="u_account",
            conn=conn,
            projections=projections,
            max_workers=multiprocessing.cpu_count()
        )

        # 使用unique_keys去重并创建最终的account表
        log_stage("数据去重", "对账户数据进行去重处理")
        unique_projections = [
            ("id", None),
            ("any_value(n1)", "n1"),
            ("any_value(n2)", "n2")
        ]

        unique_keys(
            conn=conn,
            table_name="u_account",
            projections=unique_projections
        )

        # 重命名为account表
        log_stage("表重命名", "创建最终的account表")
        execute_sql_with_timing(conn, "DROP TABLE IF EXISTS account", "删除已存在的account表")
        execute_sql_with_timing(conn, "ALTER TABLE u_u_account RENAME TO account", "重命名为account表")

        # 清理临时表
        log_stage("清理临时表", "清理临时数据表")
        execute_sql_with_timing(conn, "DROP TABLE IF EXISTS u_account", "清理临时表u_account")

        log_success("account表加载完成")
        return True

    except Exception as e:
        log_error(f"加载account表失败: {e}")
        return False


def main():
    """
    主函数:
    - 用户选择处理类型（周结或月结）
    - 获取当前工作目录
    - 列出所有子目录
    - 加载account表
    - 对每个子目录，列出Excel文件并用对应类型的处理器处理
    """
    log_stage("程序启动", "Excel操作程序开始运行")

    # 让用户选择处理类型
    processing_type = select_processing_type()
    if processing_type == 'exit':
        log_info("程序退出")
        return

    base_dir = os.getcwd()
    log_info(f"当前工作目录: {base_dir}")
    log_info(f"处理类型: {'周结' if processing_type == 'week' else '月结'}")
    entries = list_process_entries(base_dir)

    # 创建一个全局DuckDB连接，供所有处理器使用
    log_stage("数据库初始化", "初始化DuckDB数据库连接")
    db_name = f"excel_{processing_type}.db"
    db_path = os.path.join(os.getcwd(), db_name)
    log_info(f"数据库路径: {db_path}")
    global_conn = duckdb.connect(database=db_path)
    log_success("DuckDB连接创建成功")

    try:
        # 询问并加载account表
        account_loaded = load_account_table(global_conn)

        if not account_loaded:
            log_warning("未加载account表，某些功能可能受限")

        # 处理所有入口
        processing_desc = "周结" if processing_type == 'week' else "月结"
        log_stage("批量处理", f"开始{processing_desc}处理 {len(entries)} 个入口")
        processed_count = 0
        failed_count = 0

        for i, entry_dir in enumerate(entries, 1):
            try:
                log_progress(i, len(entries), f"处理入口: {os.path.basename(entry_dir)}")
                excels = list_excels(entry_dir)
                entry_name = os.path.basename(entry_dir)
                handle_entry(entry_name, entry_dir, excels, global_conn, processing_type)
                processed_count += 1
                log_success(f"入口 {entry_name} {processing_desc}处理完成")
            except Exception as e:
                failed_count += 1
                log_error(f"入口 {os.path.basename(entry_dir)} {processing_desc}处理失败: {e}")
                continue

        # 处理结果汇总
        log_stage("处理完成", f"所有{processing_desc}入口处理完毕")
        log_info(f"处理结果: 成功 {processed_count} 个，失败 {failed_count} 个")

        if processed_count > 0:
            log_success("程序执行完成，请检查输出文件")
        else:
            log_warning("没有成功处理任何入口")

    except KeyboardInterrupt:
        log_warning("用户中断程序执行")
    except Exception as e:
        log_error(f"程序执行过程中发生错误: {e}")
    finally:
        log_stage("程序结束", "清理资源并关闭连接")
        global_conn.close()
        log_success("DuckDB连接已关闭")
        log_info("程序运行结束")


if __name__ == "__main__":
    main()
