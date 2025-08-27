import os
import re
from typing import List, Dict, Tuple

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing
from excel.select_excels import select_from_excel


def zongmei_month_entry_handler(entry_dir: str, excels: List[str],
                               conn: duckdb.DuckDBPyConnection):
    """
    "6月综媒"月结入口处理函数
    - 处理综合媒体目录，包含多个不同媒体平台的消耗数据
    - 支持的媒体平台：B站、UDS、多多进宝、汇川、京准通、小红书、支付宝、变现猫
    - 自动识别各媒体平台文件并分别处理
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("综媒月结处理", f"开始处理综合媒体月结入口: {entry_name}")

    # 定义各媒体平台的文件匹配规则和字段映射
    media_configs = {
        'B站': {
            'patterns': [r'B站.*\.xlsx$'],
            'projections': [
                ('"账号id"', 'account_id'),
                ('"客户名称"', 'client_name'),
                ('"结算消耗"', 'consume_amount')
            ],
            'media_name': 'B站'
        },
        'UDS': {
            'patterns': [r'UD.*\.xls$'],
            'projections': [
                ('"媒体投放账户id"', 'account_id'),
                ('"店铺名称"', 'shop_name'),
                ('"结算消耗"', 'consume_amount')
            ],
            'media_name': 'UDS'
        },
        '多多进宝': {
            'patterns': [r'多多进宝.*\.xlsx$'],
            'projections': [
                ('"广告账户ID"', 'account_id'),
                ('"广告账户名称"', 'account_name'),
                ('"结算消耗"', 'consume_amount')
            ],
            'media_name': '多多进宝'
        },
        '汇川': {
            'patterns': [r'汇川.*\.xlsx$'],
            'projections': [
                ('"账户id"', 'account_id'),
                ('"客户名称"', 'client_name'),
                ('"结算消耗"', 'consume_amount'),
                ('"平台新客"', 'new_customer')
            ],
            'media_name': '汇川'
        },
        '京准通': {
            'patterns': [r'京准通.*\.xls$'],
            'projections': [
                ('"投放账户"', 'account_id'),
                ('"结算消耗"', 'consume_amount')
            ],
            'media_name': '京准通'
        },
        '小红书': {
            'patterns': [r'小红书.*\.xlsx$'],
            'projections': [
                ('"子账户ID"', 'account_id'),
                ('"子账户名称"', 'account_name'),
                ('"结算消耗"', 'consume_amount')
            ],
            'media_name': '小红书'
        },
        '支付宝': {
            'patterns': [r'支付宝.*\.xlsx$'],
            'projections': [
                ('"支付宝账号"', 'account_id'),
                ('"商家名称"', 'merchant_name'),
                ('"结算消耗"', 'consume_amount')
            ],
            'media_name': '支付宝'
        },
        '变现猫': {
            'patterns': [r'变现猫.*\.xls$'],
            'projections': [
                ('"广告主"', 'advertiser'),
                ('"结算消耗"', 'consume_amount')
            ],
            'media_name': '变现猫'
        }
    }

    # 阶段1: 文件发现和分类
    log_stage("文件发现", "扫描并分类各媒体平台文件")
    found_media_files: Dict[str, List[str]] = {}
    
    for media_name, config in media_configs.items():
        found_files = []
        for pattern in config['patterns']:
            matches = [f for f in excels if re.search(pattern, f, re.IGNORECASE)]
            found_files.extend(matches)
        
        if found_files:
            found_media_files[media_name] = found_files
            log_info(f"[{entry_name}] 发现{media_name}文件: {found_files}")
        else:
            log_info(f"[{entry_name}] 未发现{media_name}文件")

    if not found_media_files:
        log_error(f"[{entry_name}] 未发现任何支持的媒体平台文件")
        return

    # 过滤掉不需要处理的文件
    skip_patterns = [r'趣头条.*不做.*\.xlsx$']
    for media_name in list(found_media_files.keys()):
        filtered_files = []
        for file in found_media_files[media_name]:
            should_skip = any(re.search(pattern, file, re.IGNORECASE) for pattern in skip_patterns)
            if should_skip:
                log_info(f"[{entry_name}] 跳过文件: {file}")
            else:
                filtered_files.append(file)
        
        if filtered_files:
            found_media_files[media_name] = filtered_files
        else:
            del found_media_files[media_name]

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, f"month_{entry_name}")
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载和处理
    log_stage("数据加载", "加载各媒体平台数据")
    
    from excel.union_sheets import union_sheets_concurrent
    
    processed_tables = []
    
    for media_name, files in found_media_files.items():
        config = media_configs[media_name]
        table_name = f't_{media_name.lower()}_month'
        
        log_info(f"[{entry_name}] 处理{media_name}数据")
        
        try:
            for file in files:
                file_path = os.path.join(entry_dir, file)
                
                # 为每个文件创建临时表
                temp_table = f'{table_name}_{len(processed_tables)}'
                
                union_sheets_concurrent(
                    excel_file=file_path,
                    table_name=temp_table,
                    conn=conn,
                    projections=config['projections'],
                    max_workers=6
                )
                
                # 标准化数据格式，添加媒体平台标识
                standardized_table = f'{temp_table}_std'
                standardize_sql = create_standardize_sql(temp_table, standardized_table, config, media_name)
                
                execute_sql_with_timing(conn, standardize_sql, f"标准化{media_name}数据")
                
                processed_tables.append(standardized_table)
                
                # 清理临时表
                execute_sql_with_timing(conn, f"DROP TABLE {temp_table}", f"清理{media_name}临时表")
            
            log_success(f"[{entry_name}] {media_name}数据处理完成")
            
        except Exception as e:
            log_error(f"[{entry_name}] {media_name}数据处理失败: {e}")
            continue

    if not processed_tables:
        log_error(f"[{entry_name}] 没有成功处理任何媒体平台数据")
        return

    # 阶段4: 合并所有媒体平台数据
    log_stage("数据合并", "合并所有媒体平台数据")
    
    # 创建合并表
    union_sql = f"""
    DROP TABLE IF EXISTS t_zongmei_month;
    
    CREATE TABLE t_zongmei_month AS
    {' UNION ALL '.join([f'SELECT * FROM {table}' for table in processed_tables])};
    """
    
    try:
        execute_sql_with_timing(conn, union_sql, f"[{entry_name}] 合并综媒数据")
        log_success(f"[{entry_name}] 数据合并完成")
    except Exception as e:
        log_error(f"[{entry_name}] 数据合并失败: {e}")
        return

    # 阶段5: 导出数据
    log_stage("数据导出", "导出综媒月结数据")
    output_excel_path = output_excel.replace("\\", "\\\\")
    
    export_sql = f"""
    COPY (
        SELECT t2.n1 AS "媒体账户主体",
               COALESCE(t2.n2, '综媒客户') AS "客户",
               t1."媒体平台",
               '月结' AS "数据类型",
               t1."账户ID",
               t1."账户名称",
               '' AS "共享钱包名称",
               t1."消耗" AS "结算消耗",
               '' AS "一级行业",
               '' AS "二级行业",
               '' AS "账户类型"
        FROM t_zongmei_month AS t1
        LEFT JOIN account AS t2 ON t1."账户ID" = t2.id
        ORDER BY t1."媒体平台", t1."消耗" DESC
    ) TO '{output_excel_path}' WITH (FORMAT xlsx, HEADER true);
    """
    
    try:
        execute_sql_with_timing(conn, export_sql, f"[{entry_name}] 导出综媒数据")
        log_success(f"[{entry_name}] 综媒月结结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] 数据导出失败: {e}")
        raise
    
    # 清理临时表
    for table in processed_tables:
        try:
            execute_sql_with_timing(conn, f"DROP TABLE {table}", f"清理表{table}")
        except:
            pass


def create_standardize_sql(source_table: str, target_table: str, config: dict, media_name: str) -> str:
    """
    创建标准化SQL，将不同媒体平台的数据转换为统一格式
    """
    projections = config['projections']
    
    # 根据媒体平台定制化处理逻辑
    if media_name == 'B站':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               client_name AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '客户：' || client_name AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    elif media_name == 'UDS':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               shop_name AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '店铺：' || shop_name AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    elif media_name == '多多进宝':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               account_name AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '广告账户：' || account_name AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    elif media_name == '汇川':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               client_name AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '客户：' || client_name || CASE WHEN new_customer IS NOT NULL THEN '，新客：' || new_customer ELSE '' END AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    elif media_name == '京准通':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               account_id AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '投放账户：' || account_id AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    elif media_name == '小红书':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               account_name AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '子账户：' || account_name AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    elif media_name == '支付宝':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               merchant_name AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '商家：' || merchant_name AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    elif media_name == '变现猫':
        return f"""
        CREATE TABLE {target_table} AS
        SELECT advertiser AS "账户ID",
               advertiser AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '广告主：' || advertiser AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
    else:
        # 默认处理逻辑
        return f"""
        CREATE TABLE {target_table} AS
        SELECT account_id AS "账户ID",
               COALESCE(account_name, account_id) AS "账户名称",
               consume_amount::DOUBLE AS "消耗",
               '{media_name}' AS "媒体平台",
               '{media_name}账户' AS "备注信息"
        FROM {source_table}
        WHERE consume_amount::DOUBLE > 0.00001;
        """
