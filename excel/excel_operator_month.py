"""
excel_operator_month.py

专用于月结数据处理的Excel操作程序

使用说明:
1. 程序执行路径要求
   - 请在包含若干子目录的主目录下运行本程序。
   - 每个子目录作为一个处理入口，子目录名称即为入口名。

2. 预期输入 - 月结数据
   - 月结数据目录结构包含以下媒体平台：
   
   子目录结构：
   - 快手/：快手平台相关数据
     * 快手小牛.xlsx, 快手金牛全站.xlsx, 快手金牛非全站.xlsx
   
   - 头条/：头条平台数据
     * 头条-小牛.xlsx, 头条-赛搜全部消耗.xlsx
   
   - 广点通/：广点通平台数据
     * 广点通-乐推.xlsx, 广点通-多盟.xlsx, 广点通-小牛.xlsx
   
   - 媒体账号列表.xlsx：账户信息文件
   
   各媒体字段映射：
   - 头条：广告主账户id → 广告主公司名称 → 共享子钱包名称 → 结算消耗 → 结算一级行业 → 结算二级行业
   - 快手：账户ID → 公司名称 → 结算消耗 → 一级行业 → 二级行业 → 账户类型
   - 广点通：账户ID → 账户名称 → k框 → 结算消耗（复杂计算逻辑）

3. 输出位置
   - 处理结果输出为Excel文件，命名格式为 month_{子目录名}.xlsx。
   - 大数据量会自动分表，创建多个文件如 month_{子目录名}_part1.xlsx 等。
   - 输出文件统一保存在各子目录的上一级目录（即主目录）下。

功能:
- 专门处理月结数据，数据量大，使用并发优化
- 支持自动分表处理大数据量
- 全过程中文日志提示
- 兼容Windows和macOS

依赖:
- Python 3.7+
- duckdb (`pip install duckdb`)

打包命令:
    pyinstaller --onefile --name excel_operator_month excel_operator_month.py

用法:
    双击运行可执行文件或: python excel_operator_month.py
"""

from excel.log import (setup_logging, log_success, log_error, log_info,
                       log_warning, log_stage, log_progress, execute_sql_with_timing)
from excel.handlers.month.toutiao import toutiao_month_entry_handler
from excel.handlers.month.kuaishou_v2 import kuaishou_month_entry_handler as f4
from excel.handlers.month.kuaishou import kuaishou_month_entry_handler
from excel.handlers.month.guangdiantong_v2 import guangdiantong_v2_month_entry_handler
from excel.handlers.month.guangdiantong import guangdiantong_month_entry_handler
import duckdb
import multiprocessing
import os
import sys
from typing import List, Callable, Dict

# 确保项目根目录在 Python 模块搜索路径中
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


# 导入月结处理器

# 导入统一的日志模块

# 导入本地select模块
try:
    from excel.select_excels import select_from_excel
    from excel.union_sheets import union_sheets_concurrent, unique_keys
    print("✓ 成功导入必要模块")
except ImportError as e:
    print(f"✗ 导入模块失败: {e}")
    input("按回车键退出...")
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
month_entry_registry: Dict[str, EntryHandler] = {}


def register_month_entry(name: str, handler: EntryHandler):
    """
    注册月结入口处理函数
    """
    month_entry_registry[name] = handler


def handle_entry(entry_name: str, entry_dir: str, excels: List[str],
                 conn: duckdb.DuckDBPyConnection):
    """
    调用指定入口名的处理函数
    """
    log_stage("入口处理", f"开始处理月结入口: {entry_name}")

    handler = None
    matched_key = None

    # 首先尝试精确匹配
    if entry_name in month_entry_registry:
        handler = month_entry_registry[entry_name]
        matched_key = entry_name
    else:
        # 如果精确匹配失败，尝试startswith匹配
        for handler_key in month_entry_registry:
            if entry_name.startswith(handler_key):
                handler = month_entry_registry[handler_key]
                matched_key = handler_key
                break

    if not handler:
        log_warning(f"未注册月结入口处理器: '{entry_name}'")
        return

    log_info(f"[{entry_name}] 使用月结处理器: {matched_key}")

    try:
        handler(entry_dir, excels, conn)
        log_success(f"[{entry_name}] 月结入口处理完成")
    except Exception as e:
        log_error(f"[{entry_name}] 月结入口处理失败: {e}")
        raise


# 注册月结处理器
register_month_entry("广点通", guangdiantong_month_entry_handler)
register_month_entry("广点通-大端口", guangdiantong_v2_month_entry_handler)
register_month_entry("快手", kuaishou_month_entry_handler)
register_month_entry("头条", toutiao_month_entry_handler)
register_month_entry("快手大健康金牛全站-执象", f4)


def load_account_table(conn: duckdb.DuckDBPyConnection, base_dir: str) -> bool:
    """
    加载account表的函数，默认从可执行文件所在目录查找媒体账户Excel文件
    """
    log_stage("账户表加载", "准备加载媒体账户表")

    # 在可执行文件所在目录查找媒体账户文件
    potential_files = []
    try:
        for file in os.listdir(base_dir):
            if (file.lower().endswith('.xlsx') and
                    ('媒体账户' in file or '媒体账号' in file or '账户列表' in file or '账号列表' in file)):
                potential_files.append(file)
    except Exception as e:
        log_error(f"扫描目录失败: {e}")
        return False

    excel_file = None

    if len(potential_files) == 0:
        log_warning("未在可执行文件所在目录找到媒体账户Excel文件")
        log_info("提示：文件名应包含'媒体账户'、'媒体账号'、'账户列表'或'账号列表'等关键词")

        # 询问用户是否手动输入路径
        while True:
            choice = input("是否手动输入媒体账户Excel文件路径？(y/n): ").strip().lower()
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

    elif len(potential_files) == 1:
        excel_file = os.path.join(base_dir, potential_files[0])
        log_success(f"自动找到媒体账户文件: {potential_files[0]}")

    else:
        log_info(f"发现多个可能的媒体账户文件: {potential_files}")
        print("发现多个可能的媒体账户文件:")
        for i, file in enumerate(potential_files, 1):
            print(f"  {i}. {file}")

        while True:
            try:
                choice = int(
                    input(f"请选择要使用的文件 (1-{len(potential_files)}): ").strip())
                if 1 <= choice <= len(potential_files):
                    excel_file = os.path.join(
                        base_dir, potential_files[choice-1])
                    log_success(f"选择文件: {potential_files[choice-1]}")
                    break
                else:
                    print(f"请输入 1 到 {len(potential_files)} 之间的数字")
            except ValueError:
                print("请输入有效的数字")

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
            ('"客户名称"', "n2"),
            ('"客户编号"', "n3")
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
            ("any_value(n2)", "n2"),
            ("any_value(n3)", "n3")
        ]

        unique_keys(
            conn=conn,
            table_name="u_account",
            projections=unique_projections
        )

        # 重命名为account表
        log_stage("表重命名", "创建最终的account表")
        execute_sql_with_timing(
            conn, "DROP TABLE IF EXISTS account", "删除已存在的account表")
        execute_sql_with_timing(
            conn, "ALTER TABLE u_u_account RENAME TO account", "重命名为account表")

        # 清理临时表
        log_stage("清理临时表", "清理临时数据表")
        execute_sql_with_timing(
            conn, "DROP TABLE IF EXISTS u_account", "清理临时表u_account")

        log_success("account表加载完成")
        return True

    except Exception as e:
        log_error(f"加载account表失败: {e}")
        return False


def main():
    """
    月结数据处理主函数
    """
    print("=" * 60)
    print("    Excel月结数据处理程序")
    print("    专门处理月结数据，数据量大，使用并发优化")
    print("=" * 60)
    print()

    # 获取可执行文件所在目录作为工作目录
    if getattr(sys, 'frozen', False):
        # 如果是打包后的可执行文件，默认使用可执行文件所在目录
        executable_dir = os.path.dirname(sys.executable)
        base_dir = executable_dir
        os.chdir(base_dir)
    else:
        # 如果是Python脚本直接运行
        base_dir = os.getcwd()

    # 设置统一的日志配置，在确定工作目录后
    log_file_path = os.path.join(base_dir, "excel_month.log")
    logger = setup_logging(log_file=log_file_path)

    log_stage("程序启动", "Excel月结数据处理程序开始运行")

    if getattr(sys, 'frozen', False):
        log_info(f"检测到打包环境，可执行文件路径: {sys.executable}")
        log_info(f"可执行文件所在目录: {executable_dir}")
        log_success(f"已自动切换到可执行文件所在目录: {base_dir}")
    else:
        log_info(f"检测到脚本环境，使用当前工作目录: {base_dir}")

    log_info(f"最终工作目录: {base_dir}")
    log_info("处理类型: 月结数据")
    entries = list_process_entries(base_dir)

    # 创建一个全局DuckDB连接，供所有处理器使用
    log_stage("数据库初始化", "初始化DuckDB数据库连接")
    db_path = os.path.join(base_dir, "excel_month.db")
    log_info(f"数据库路径: {db_path}")
    global_conn = duckdb.connect(database=db_path)
    log_success("DuckDB连接创建成功")

    try:
        # 自动加载account表
        account_loaded = load_account_table(global_conn, base_dir)

        if not account_loaded:
            log_warning("未加载account表，某些功能可能受限")

        # 处理所有入口
        log_stage("批量处理", f"开始月结处理 {len(entries)} 个入口")
        processed_count = 0
        failed_count = 0

        for i, entry_dir in enumerate(entries, 1):
            try:
                log_progress(i, len(entries),
                             f"处理入口: {os.path.basename(entry_dir)}")
                excels = list_excels(entry_dir)
                entry_name = os.path.basename(entry_dir)
                handle_entry(entry_name, entry_dir, excels, global_conn)
                processed_count += 1
                log_success(f"入口 {entry_name} 月结处理完成")
            except Exception as e:
                failed_count += 1
                log_error(f"入口 {os.path.basename(entry_dir)} 月结处理失败: {e}")
                continue

        # 处理结果汇总
        log_stage("处理完成", f"所有月结入口处理完毕")
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

        # 等待用户按键后退出
        input("\n按回车键退出程序...")


if __name__ == "__main__":
    main()
