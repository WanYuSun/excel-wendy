import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing
from excel.select_excels import select_from_excel


def guangdiantong_v2_month_entry_handler(entry_dir: str, excels: List[str],
                                         conn: duckdb.DuckDBPyConnection):
    """
    "广点通-大端口"月结入口处理函数
    - 专门处理大端口月结数据，数据量更大，sheet数量更多
    - 优化的数据处理逻辑，适合大数据量场景
    - 查找所需Excel文件（正则匹配），如未找到则提示用户输入文件名
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("广点通大端口月结处理", f"开始处理广点通大端口月结入口: {entry_name}")

    # 提取subentry名称
    dash_index = entry_name.find('-')
    if dash_index == -1:
        log_error(f"[{entry_name}] entry_name格式错误，应为entry-subentry格式")
        return
    subentry_name = entry_name[dash_index + 1:]
    log_info(f"[{entry_name}] 子入口名称: {subentry_name}")

    # 阶段1: 文件查找和选择
    log_stage("文件查找", f"查找{entry_name}主数据文件")
    required = [
        (rf'^{re.escape(entry_name)}.*\.xlsx$',
         f"未找到{entry_name}主数据文件，请手动输入文件名"),
    ]
    try:
        found = []
        for pattern, prompt_msg in required:
            matches = [x for x in excels if re.match(pattern, x)]
            f = select_excel_from_matches(matches, entry_dir, prompt_msg)
            found.append(f)
        main_excel = found[0]
        log_success(f"[{entry_name}] 找到主数据文件: {os.path.basename(main_excel)}")
    except SkipEntryException as e:
        log_info(f"[{entry_name}] {e}")
        return

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, f"month_{entry_name}")
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载（大端口月结数据处理，使用高并发）
    log_stage("数据加载", "从Excel文件加载大端口月结数据到临时表（使用高并发处理）")
    t_g2 = 't_g2_month'
    try:
        from excel.union_sheets import union_sheets_concurrent
        
        # 大端口数据字段根据新需求调整，包含所有消耗类型
        projections = [
            ('"账户ID"', 'account_id'),
            ('"账户名称"', 'account_name'),
            ('COALESCE("k框", "服务商简称")', 'k_box'),
            ('"现金消耗（元）"', 'cash_consume'),
            ('"信用金消耗（元）"', 'credit_consume'),
            ('"赠送金消耗（元）"', 'gift_consume'),
            ('"红包封面消耗"', 'red_envelope_consume'),
            ('"微信内购赠送金消耗"', 'wechat_gift_consume'),
            ('"微信内购快周转消耗"', 'wechat_quick_consume'),
            ('"专用金消耗"', 'special_consume'),
            ('"补偿虚拟金消耗"', 'compensation_consume'),
            ('"安卓定向应用金消耗"', 'android_app_consume'),
            ('"TCC赠送金消耗（微信广告）"', 'tcc_gift_consume'),
            ('"微信专用小游戏抵用金消耗"', 'wechat_game_consume'),
            ('"互选广告消耗"', 'mutual_ad_consume'),
            ('"流量主广告金消耗"', 'traffic_ad_consume'),
            ('"短剧内购赠送金消耗"', 'drama_gift_consume')
        ]
        
        union_sheets_concurrent(
            excel_file=main_excel,
            table_name=t_g2,
            conn=conn,
            projections=projections,
            max_workers=12  # 大端口数据量特别大，使用更多线程
        )
        
        log_success(f"[{entry_name}] 大端口月结数据加载完成，表名: {t_g2}")
    except Exception as e:
        log_error(f"[{entry_name}] 大端口月结数据加载失败: {e}")
        return

    # SQL模板，针对大端口月结数据优化，使用新的消耗计算逻辑
    sql_template = """
-- 广点通大端口月结数据处理

DROP TABLE IF EXISTS t_guang_v2_month;

-- 数据预处理和清洗，先按账户ID聚合各项消耗，然后计算结算消耗
-- 结算消耗 = 现金消耗（元）+ 信用金消耗（元）+ 赠送金消耗（元）- 红包封面消耗 - 微信内购赠送金消耗 - 微信内购快周转消耗 - 专用金消耗 - 补偿虚拟金消耗 - 安卓定向应用金消耗 - TCC赠送金消耗（微信广告）- 微信专用小游戏抵用金消耗 - 互选广告消耗 - 流量主广告金消耗 - 短剧内购赠送金消耗
CREATE TABLE t_guang_v2_month AS
SELECT account_id AS "账户ID",
       any_value(account_name) AS "账户名称",
       any_value(t2.n2) AS "客户名称",  -- 从媒体账户表获取客户名称
       any_value(k_box) AS "k框",
       (
           sum(COALESCE(cash_consume::DOUBLE, 0)) + 
           sum(COALESCE(credit_consume::DOUBLE, 0)) + 
           sum(COALESCE(gift_consume::DOUBLE, 0)) - 
           sum(COALESCE(red_envelope_consume::DOUBLE, 0)) - 
           sum(COALESCE(wechat_gift_consume::DOUBLE, 0)) - 
           sum(COALESCE(wechat_quick_consume::DOUBLE, 0)) - 
           sum(COALESCE(special_consume::DOUBLE, 0)) - 
           sum(COALESCE(compensation_consume::DOUBLE, 0)) - 
           sum(COALESCE(android_app_consume::DOUBLE, 0)) - 
           sum(COALESCE(tcc_gift_consume::DOUBLE, 0)) - 
           sum(COALESCE(wechat_game_consume::DOUBLE, 0)) - 
           sum(COALESCE(mutual_ad_consume::DOUBLE, 0)) - 
           sum(COALESCE(traffic_ad_consume::DOUBLE, 0)) - 
           sum(COALESCE(drama_gift_consume::DOUBLE, 0))
       ) AS "结算消耗"
FROM {table_name} AS t1
LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)
GROUP BY account_id;

-- 创建优化索引提升查询性能
CREATE INDEX IF NOT EXISTS idx_account_id ON t_guang_v2_month("账户ID");
"""
    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行大端口月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")
    log_info(f"[{entry_name}] 输出路径: {output_excel_path}")

    # 首先检查account表是否存在
    try:
        conn.execute("SELECT COUNT(*) FROM account")
        result = conn.fetchone()[0]
        log_info(f"[{entry_name}] account表包含 {result} 条记录")
    except Exception as e:
        log_warning(f"[{entry_name}] account表不存在或无法访问: {e}")
        log_info(f"[{entry_name}] 将不进行客户名称关联，使用空值填充")
        # 修改SQL模板，移除account表关联
        sql_template = sql_template.replace(
            'LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)',
            ''
        ).replace(
            'any_value(t2.n2) AS "客户名称",  -- 从媒体账户表获取客户名称',
            'NULL AS "客户名称",  -- account表不存在，使用NULL'
        )

    sql = sql_template.format(
        table_name=t_g2,
        output_excel=output_excel_path,
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行大端口月结数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 广点通大端口月结数据处理")
        
        # 检查最终数据量，决定输出策略
        try:
            conn.execute("SELECT COUNT(*) FROM t_guang_v2_month")
            final_row_count = conn.fetchone()[0]
            log_info(f"[{entry_name}] 汇总后数据量: {final_row_count} 行")
            
            # 如果数据量超过50000行，考虑分sheet处理
            if final_row_count > 50000:
                log_info(f"[{entry_name}] 数据量较大({final_row_count}行)，将在单个Excel文件中创建多个sheet")
                sheets_needed = (final_row_count + 49999) // 50000  # 每个sheet最多50000行
                log_info(f"[{entry_name}] 预计需要 {sheets_needed} 个sheet")
                
                # 由于DuckDB的COPY命令限制，我们先分别导出为临时文件
                temp_files = []
                
                # 分批导出到临时文件
                for sheet_num in range(sheets_needed):
                    offset = sheet_num * 50000
                    temp_file = output_excel.replace('.xlsx', f'_temp_sheet{sheet_num + 1}.xlsx')
                    temp_file_path = temp_file.replace("\\", "\\\\")
                    temp_files.append(temp_file)
                    
                    export_sql = f"""
COPY
  (SELECT "账户ID",
          "账户名称",
          "客户名称",
          "k框",
          "结算消耗"
   FROM t_guang_v2_month
   LIMIT 50000 OFFSET {offset}) TO '{temp_file_path}' WITH (FORMAT xlsx, HEADER true);
"""
                    
                    execute_sql_with_timing(conn, export_sql, f"[{entry_name}] 导出第{sheet_num + 1}个临时sheet")
                    log_info(f"[{entry_name}] 第{sheet_num + 1}个临时sheet已创建")
                
                # 保持分离的文件，不再尝试合并
                log_info(f"[{entry_name}] 数据量较大，保持分离的Excel文件以避免合并问题")
                log_info(f"[{entry_name}] 已创建 {len(temp_files)} 个分离的Excel文件")
                
                for i, temp_file in enumerate(temp_files, 1):
                    final_name = output_excel.replace('.xlsx', f'_part{i}.xlsx')
                    if os.path.exists(temp_file):
                        try:
                            os.rename(temp_file, final_name)
                            log_success(f"[{entry_name}] 文件已重命名: {os.path.basename(final_name)}")
                        except:
                            log_warning(f"[{entry_name}] 无法重命名文件: {temp_file}")
                
                log_success(f"[{entry_name}] 广点通大端口月结数据已分离到 {sheets_needed} 个Excel文件，总计 {final_row_count} 行数据")
                    
            else:
                # 数据量不大，单个文件单个sheet输出
                export_sql = f"""
-- 导出大端口月结数据
COPY
  (SELECT "账户ID",
          "账户名称",
          "客户名称",
          "k框",
          "结算消耗"
   FROM t_guang_v2_month) TO '{output_excel_path}' WITH (FORMAT xlsx, HEADER true);
"""
                execute_sql_with_timing(conn, export_sql, f"[{entry_name}] 导出广点通大端口月结数据")
                log_success(f"[{entry_name}] 大端口月结结果已输出到: {output_excel}")
                
        except Exception as export_e:
            log_error(f"[{entry_name}] 数据导出失败: {export_e}")
            raise
        
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
