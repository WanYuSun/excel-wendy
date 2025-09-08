import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, log_warning, execute_sql_with_timing
from excel.select_excels import select_from_excel


def toutiao_month_entry_handler(entry_dir: str, excels: List[str],
                                conn: duckdb.DuckDBPyConnection):
    """
    "头条"月结入口处理函数
    - 专门处理月结数据，数据量更大，sheet数量更多
    - 查找所需Excel文件（简化匹配规则，不再需要数字结尾）
    - 字段储存：广告主账户id、广告主账户名称、广告主公司名称、共享子钱包名称、非赠款消耗、返佣消耗、结算一级行业、结算二级行业、总消耗
    - 与媒体账户表联合：头条中的广告主账户id 对应 媒体账户表的账号ID
    - 最终输出：客户名称、客户编号、广告主公司名称、结算消耗、账户ID、广告主账户id、广告主账户名称、共享子钱包名称、总消耗、结算一级行业、结算二级行业
    - 计算逻辑：结算消耗 = 非赠款消耗 - 返佣消耗
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("头条月结处理", f"开始处理头条月结入口: {entry_name}")

    # 阶段1: 文件查找和选择（简化匹配规则）
    log_stage("文件查找", f"查找{entry_name}相关文件")
    try:
        # 简化文件匹配规则，匹配所有头条相关文件
        toutiao_pattern = r'头条.*\.xlsx$'
        toutiao_matches = [x for x in excels if re.search(toutiao_pattern, x, re.IGNORECASE)]
        
        if not toutiao_matches:
            raise SkipEntryException(f"未找到头条相关文件")
        
        log_info(f"[{entry_name}] 发现头条文件: {toutiao_matches}")
        
        # 如果有多个文件，询问用户是否全部使用
        if len(toutiao_matches) == 1:
            toutiao_excels = [os.path.join(entry_dir, toutiao_matches[0])]
        else:
            log_info(f"[{entry_name}] 发现 {len(toutiao_matches)} 个头条文件")
            choice = input("是否使用所有头条文件进行合并处理？(y/n): ").strip().lower()
            if choice in ['y', 'yes', '是']:
                toutiao_excels = [os.path.join(entry_dir, f) for f in toutiao_matches]
            else:
                # 让用户选择单个文件
                selected_file = select_excel_from_matches(
                    toutiao_matches, entry_dir,
                    f"请选择要使用的头条文件"
                )
                toutiao_excels = [selected_file]

        log_success(f"[{entry_name}] 头条文件确定完成，共{len(toutiao_excels)}个文件")

    except SkipEntryException as e:
        log_info(f"[{entry_name}] {e}")
        return

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, f"month_{entry_name}")
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载
    log_stage("数据加载", "从Excel文件加载头条月结数据到临时表")
    
    from excel.union_sheets import union_sheets_concurrent
    
    # 头条字段映射：根据新需求调整字段
    # 需要存储：广告主账户id、共享子钱包名称、非赠款消耗、返佣消耗、结算一级行业、结算二级行业
    toutiao_projections = [
        ('"广告主账户id"', 'advertiser_account_id'),
        ('"广告主账户名称"', 'advertiser_account_name'),
        ('"广告主公司名称"', 'advertiser_company_name'),
        ('"共享子钱包名称"', 'shared_wallet_name'),
        ('"非赠款消耗"', 'non_gift_consume'),
        ('"返佣消耗"', 'rebate_consume'),
        ('"结算一级行业"', 'settle_industry_level1'),
        ('"结算二级行业"', 'settle_industry_level2'),
        ('"总消耗"', 'total_consume'),
        ('"一级代理商账户名称"', 'first_level_agent_account_name')
    ]
    
    t_toutiao = 't_toutiao_month'
    try:
        # 处理第一个文件
        union_sheets_concurrent(
            excel_file=toutiao_excels[0],
            table_name=t_toutiao,
            conn=conn,
            projections=toutiao_projections,
            max_workers=8
        )
        
        # 处理其余文件并合并
        for i, file in enumerate(toutiao_excels[1:], 2):
            temp_table = f't_toutiao_temp_{i}'
            union_sheets_concurrent(
                excel_file=file,
                table_name=temp_table,
                conn=conn,
                projections=toutiao_projections,
                max_workers=8
            )
            
            # 合并到主表
            execute_sql_with_timing(
                conn,
                f"INSERT INTO {t_toutiao} SELECT * FROM {temp_table}",
                f"合并头条文件 {i}"
            )
            
            # 清理临时表
            execute_sql_with_timing(
                conn,
                f"DROP TABLE {temp_table}",
                f"清理临时表 {temp_table}"
            )
        
        log_success(f"[{entry_name}] 头条数据加载完成")
    except Exception as e:
        log_error(f"[{entry_name}] 头条数据加载失败: {e}")
        return

    # SQL模板：根据新需求调整汇总逻辑
    sql_template = """
    -- 头条月结数据处理
    DROP TABLE IF EXISTS t_toutiao_month_final;

    CREATE TABLE t_toutiao_month_final AS
    SELECT t1.advertiser_account_id AS "广告主账户id",
           any_value(t1.advertiser_company_name) AS "广告主公司名称",
           any_value(t2.n2) AS "客户名称",  -- 客户名称
           any_value(t2.n3) AS "客户编号",  -- 客户编号
           (sum(COALESCE(t1.non_gift_consume::DOUBLE, 0)) - sum(COALESCE(t1.rebate_consume::DOUBLE, 0))) AS "结算消耗",
           any_value(t1.advertiser_account_id) AS "账户ID",
           any_value(t1.advertiser_account_name) AS "广告主账户名称",
           any_value(t1.shared_wallet_name) AS "共享子钱包名称",
           sum(COALESCE(t1.total_consume::DOUBLE, 0)) AS "总消耗",
           any_value(t1.settle_industry_level1) AS "结算一级行业",
           any_value(t1.settle_industry_level2) AS "结算二级行业",
           any_value(t1.first_level_agent_account_name) AS "一级代理商账户名称"
    FROM {toutiao_table} AS t1
    LEFT JOIN account AS t2 ON CAST(t1.advertiser_account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)
    GROUP BY t1.advertiser_account_id;
    """

    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行头条月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")

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
            'LEFT JOIN account AS t2 ON CAST(t1.advertiser_account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)',
            ''
        ).replace(
            'any_value(t2.n2) AS "客户名称",  -- 从媒体账户表获取客户名称',
            'NULL AS "客户名称",  -- account表不存在，使用NULL'
        ).replace(
            'any_value(t2.n3) AS "客户编号",  -- 客户编号',
            'NULL AS "客户编号",  -- account表不存在，使用NULL'
        )

    # 执行数据汇总
    sql = sql_template.format(toutiao_table=t_toutiao)
    execute_sql_with_timing(conn, sql, f"[{entry_name}] 头条数据汇总")

    # 检查最终数据量，决定输出策略
    try:
        conn.execute("SELECT COUNT(*) FROM t_toutiao_month_final")
        final_row_count = conn.fetchone()[0]
        log_info(f"[{entry_name}] 汇总后数据量: {final_row_count} 行")
        
        # 如果数据量超过50000行，考虑分sheet处理
        if final_row_count > 50000:
            log_info(f"[{entry_name}] 数据量较大({final_row_count}行)，将在单个Excel文件中创建多个sheet")
            sheets_needed = (final_row_count + 49999) // 50000  # 每个sheet最多50000行
            log_info(f"[{entry_name}] 预计需要 {sheets_needed} 个sheet")
            
            # 由于DuckDB的COPY命令限制，我们先分别导出为临时文件，然后合并
            temp_files = []
            
            # 分批导出到临时文件
            for sheet_num in range(sheets_needed):
                offset = sheet_num * 50000
                temp_file = output_excel.replace('.xlsx', f'_temp_sheet{sheet_num + 1}.xlsx')
                temp_file_path = temp_file.replace("\\", "\\\\")
                temp_files.append(temp_file)

                export_sql = f"""
COPY
  (SELECT "客户名称",
          "客户编号",
          "广告主公司名称",
          "结算消耗",
          "账户ID",
          "广告主账户id",
          "广告主账户名称",
          "共享子钱包名称",
          "总消耗",
          "结算一级行业",
          "结算二级行业",
          "一级代理商账户名称"
   FROM t_toutiao_month_final
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
            
            log_success(f"[{entry_name}] 头条月结数据已分离到 {sheets_needed} 个Excel文件，总计 {final_row_count} 行数据")
                
        else:
            # 数据量不大，单个文件单个sheet输出
            export_sql = f"""
-- 导出头条月结数据
COPY
  (SELECT "客户名称",
          "客户编号",
          "广告主公司名称",
          "结算消耗",
          "账户ID",
          "广告主账户id",
          "广告主账户名称",
          "共享子钱包名称",
          "总消耗",
          "结算一级行业",
          "结算二级行业",
          "一级代理商账户名称"
   FROM t_toutiao_month_final) TO '{output_excel_path}' WITH (FORMAT xlsx, HEADER true);
"""
            execute_sql_with_timing(conn, export_sql, f"[{entry_name}] 导出头条月结数据")
            log_success(f"[{entry_name}] 头条月结结果已输出到: {output_excel}")
            
    except Exception as export_e:
        log_error(f"[{entry_name}] 数据导出失败: {export_e}")
        raise
    
    # 最终完成日志
    log_stage("处理完成", f"头条月结数据处理完成，共处理 {final_row_count} 行数据")
