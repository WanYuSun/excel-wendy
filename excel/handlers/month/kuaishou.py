import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, log_warning, execute_sql_with_timing
from excel.select_excels import select_from_excel


def kuaishou_month_entry_handler(entry_dir: str, excels: List[str],
                                 conn: duckdb.DuckDBPyConnection):
    """
    "快手"月结入口处理函数
    - 专门处理月结数据，数据量更大，sheet数量更多
    - 查找所需Excel文件（简化匹配规则，不再需要数字结尾）
    - 字段储存：账户ID、公司名称、账户类型、一级行业、二级行业、用户自选类目、产品名、素材打标类目、框返花费(元)、总消耗、现金花费(元)、信用花费(元)、前返花费(元)、后返花费(元)
    - 与媒体账户表联合：快手中的账户ID 对应 媒体账户表的账号ID
    - 最终输出：客户名称、客户编号、公司名称、结算消耗、账户ID、账户类型、一级行业、二级行业、产品名、用户自选类目、素材打标类目、框返花费(元)、总消耗
    - 计算逻辑：结算消耗 = 现金花费(元) + 信用花费(元) + 前返花费(元) + 后返花费(元)
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("快手月结处理", f"开始处理快手月结入口: {entry_name}")

    # 阶段1: 文件查找和选择（简化匹配规则）
    log_stage("文件查找", f"查找{entry_name}相关文件")
    try:
        # 简化文件匹配规则，匹配所有快手相关文件
        kuaishou_pattern = r'快手.*\.xlsx$'
        kuaishou_matches = [x for x in excels if re.search(kuaishou_pattern, x, re.IGNORECASE)]
        
        if not kuaishou_matches:
            raise SkipEntryException(f"未找到快手相关文件")
        
        log_info(f"[{entry_name}] 发现快手文件: {kuaishou_matches}")
        
        # 如果有多个文件，询问用户是否全部使用
        if len(kuaishou_matches) == 1:
            kuaishou_excels = [os.path.join(entry_dir, kuaishou_matches[0])]
        else:
            log_info(f"[{entry_name}] 发现 {len(kuaishou_matches)} 个快手文件")
            choice = input("是否使用所有快手文件进行合并处理？(y/n): ").strip().lower()
            if choice in ['y', 'yes', '是']:
                kuaishou_excels = [os.path.join(entry_dir, f) for f in kuaishou_matches]
            else:
                # 让用户选择单个文件
                selected_file = select_excel_from_matches(
                    kuaishou_matches, entry_dir,
                    f"请选择要使用的快手文件"
                )
                kuaishou_excels = [selected_file]

        log_success(f"[{entry_name}] 快手文件确定完成，共{len(kuaishou_excels)}个文件")

    except SkipEntryException as e:
        log_info(f"[{entry_name}] {e}")
        return

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, f"month_{entry_name}")
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载
    log_stage("数据加载", "从Excel文件加载快手月结数据到临时表")
    
    from excel.union_sheets import union_sheets_concurrent
    
    def safe_process_excel_file(excel_file: str, table_name: str, projections_list: List, entry_name: str):
        """
        安全处理Excel文件的函数，支持多套字段映射和容错处理
        """
        file_name = os.path.basename(excel_file)
        
        for i, (projections, proj_name) in enumerate(projections_list):
            try:
                log_info(f"[{entry_name}] 尝试使用{proj_name}处理文件: {file_name}")
                
                union_sheets_concurrent(
                    excel_file=excel_file,
                    table_name=table_name,
                    conn=conn,
                    projections=projections,
                    max_workers=8
                )
                
                # 检查是否成功加载了数据
                try:
                    conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                    loaded_rows = conn.fetchone()[0]
                    if loaded_rows > 0:
                        log_success(f"[{entry_name}] 使用{proj_name}成功处理文件 {file_name}，加载 {loaded_rows} 行数据")
                        return proj_name, projections
                    else:
                        log_warning(f"[{entry_name}] {proj_name}处理成功但未加载到数据，尝试下一套映射")
                        # 清理空表
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        continue
                except Exception as count_e:
                    log_warning(f"[{entry_name}] 无法检查数据加载情况: {count_e}")
                    return proj_name, projections
                    
            except Exception as proj_error:
                error_msg = str(proj_error).lower()
                if "no rows found" in error_msg or "not found" in error_msg or "empty" in error_msg:
                    log_warning(f"[{entry_name}] {proj_name}未找到数据，尝试下一套映射")
                    # 清理可能创建的空表
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except:
                        pass
                    continue
                else:
                    log_warning(f"[{entry_name}] {proj_name}处理失败: {proj_error}")
                    # 清理可能创建的问题表
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except:
                        pass
                    continue
        
        # 所有映射都失败了
        raise Exception(f"所有字段映射都无法处理文件 {file_name}，可能文件为空或字段名完全不匹配")
    
    # 快手字段映射：根据新需求调整字段
    # 需要存储：账户ID、公司名称、账户类型、一级行业、二级行业、现金花费(元)、信用花费(元)、前返花费(元)、后返花费(元)
    kuaishou_projections = [
        ('"账户ID"', 'account_id'),
        ('"公司名称"', 'company_name'),
        ('"账户类型"', 'account_type'),
        ('"一级行业"', 'industry_level1'),
        ('"二级行业"', 'industry_level2'),
        ('"用户自选类目"', 'user_choice_category'),
        ('"产品名"', 'product_name'),
        ('"素材打标类目"', 'material_label_category'),
        ('"框返花费(元)"', 'frame_rebate_cost'),
        ('"总消耗"', 'total_consume'),
        ('"现金花费(元)"', 'cash_cost'),
        ('"信用花费(元)"', 'credit_cost'),
        ('"前返花费(元)"', 'front_rebate_cost'),
        ('"后返花费(元)"', 'back_rebate_cost')
    ]
    
    t_kuaishou = 't_kuaishou_month'
    
    # 准备所有可用的字段映射策略
    projection_strategies = [
        (kuaishou_projections, "主要字段映射")
    ]
    
    try:
        # 处理第一个文件
        log_info(f"[{entry_name}] 开始处理第一个文件: {os.path.basename(kuaishou_excels[0])}")
        
        try:
            used_projection_name, used_projections = safe_process_excel_file(
                kuaishou_excels[0], t_kuaishou, projection_strategies, entry_name
            )
            log_success(f"[{entry_name}] 第一个文件处理完成，使用了{used_projection_name}")
        except Exception as first_file_error:
            log_error(f"[{entry_name}] 第一个文件处理完全失败: {first_file_error}")
            
            # 尝试检查文件是否存在且可读
            if os.path.exists(kuaishou_excels[0]):
                file_size = os.path.getsize(kuaishou_excels[0])
                log_info(f"[{entry_name}] 文件存在，大小: {file_size} 字节")
                if file_size < 1024:
                    log_warning(f"[{entry_name}] 文件很小，可能是空文件")
            else:
                log_error(f"[{entry_name}] 文件不存在: {kuaishou_excels[0]}")
            return
        
        # 处理其余文件并合并
        successful_files = 1
        failed_files = 0
        
        for i, file in enumerate(kuaishou_excels[1:], 2):
            log_info(f"[{entry_name}] 开始处理第{i}个文件: {os.path.basename(file)}")
            temp_table = f't_kuaishou_temp_{i}'
            
            try:
                # 使用相同的字段映射策略处理后续文件
                temp_projection_name, _ = safe_process_excel_file(
                    file, temp_table, [(used_projections, used_projection_name)], entry_name
                )
                
                # 合并到主表
                execute_sql_with_timing(
                    conn,
                    f"INSERT INTO {t_kuaishou} SELECT * FROM {temp_table}",
                    f"合并快手文件 {i}"
                )
                
                # 清理临时表
                execute_sql_with_timing(
                    conn,
                    f"DROP TABLE {temp_table}",
                    f"清理临时表 {temp_table}"
                )
                successful_files += 1
                log_success(f"[{entry_name}] 第{i}个文件处理完成")
                
            except Exception as file_error:
                failed_files += 1
                log_warning(f"[{entry_name}] 第{i}个文件处理失败，跳过: {file_error}")
                # 清理可能创建的临时表
                try:
                    execute_sql_with_timing(
                        conn,
                        f"DROP TABLE IF EXISTS {temp_table}",
                        f"清理失败的临时表 {temp_table}"
                    )
                except:
                    pass
                continue
        
        # 最终数据检查
        try:
            conn.execute(f"SELECT COUNT(*) FROM {t_kuaishou}")
            row_count = conn.fetchone()[0]
            log_info(f"[{entry_name}] 文件处理汇总: 成功 {successful_files} 个，失败 {failed_files} 个")
            log_info(f"[{entry_name}] 最终加载数据: {row_count} 行")
            
            if row_count == 0:
                log_error(f"[{entry_name}] 所有文件处理后仍然没有数据，请检查文件内容")
                return
            elif row_count > 100000:
                log_info(f"[{entry_name}] 数据量较大({row_count}行)，输出时将分多个文件处理")
                
        except Exception as count_e:
            log_warning(f"[{entry_name}] 无法统计最终数据行数: {count_e}")
        
        log_success(f"[{entry_name}] 快手数据加载完成，使用了{used_projection_name}")
        
    except Exception as e:
        log_error(f"[{entry_name}] 快手数据加载失败: {e}")
        
        # 提供更详细的错误信息和建议
        log_error(f"[{entry_name}] 详细错误分析:")
        log_error(f"  - 所有文件都无法正常处理")
        log_error(f"  - 尝试过的字段映射策略:")
        for projections, name in projection_strategies:
            log_error(f"    * {name}")
        log_error(f"  - 建议:")
        log_error(f"    1. 检查Excel文件是否损坏")
        log_error(f"    2. 确认文件中有数据且不全为空sheet")
        log_error(f"    3. 检查字段名是否包含特殊字符")
        log_error(f"    4. 尝试手动打开Excel文件确认内容")
        
        return

    # SQL模板：根据新需求调整汇总逻辑
    sql_template = """
-- 快手月结数据处理：结算消耗 = 现金花费(元) + 信用花费(元) + 前返花费(元) + 后返花费(元)

DROP TABLE IF EXISTS t_kuaishou_month_final;
-- 汇总快手数据，先按账户ID聚合各项花费，然后计算结算消耗，并与媒体账户表关联
CREATE TABLE t_kuaishou_month_final AS
SELECT t1.account_id AS "账户ID",
       any_value(t2.n2) AS "客户名称",  -- 从媒体账户表获取客户名称
       any_value(t2.n3) AS "客户编号",  -- 从媒体账户表获取客户编号
       any_value(t1.company_name) AS "公司名称",
        (sum(COALESCE(t1.cash_cost::DOUBLE, 0)) + sum(COALESCE(t1.credit_cost::DOUBLE, 0)) + 
        sum(COALESCE(t1.front_rebate_cost::DOUBLE, 0)) + sum(COALESCE(t1.back_rebate_cost::DOUBLE, 0))) AS "结算消耗",  -- 结算消耗 = 四种花费分别求和后相加
       any_value(t1.account_type) AS "账户类型",
       any_value(t1.industry_level1) AS "一级行业",
       any_value(t1.industry_level2) AS "二级行业",
       any_value(t1.product_name) AS "产品名",
       any_value(t1.user_choice_category) AS "用户自选类目",
       any_value(t1.material_label_category) AS "素材打标类目",
       sum(COALESCE(t1.frame_rebate_cost::DOUBLE, 0)) AS "框返花费(元)",
       sum(COALESCE(t1.total_consume::DOUBLE, 0)) AS "总消耗"
FROM {kuaishou_table} AS t1
LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)  -- 确保数据类型匹配
GROUP BY t1.account_id
ORDER BY "结算消耗" DESC;
"""

    # 检查数据量，决定输出策略
    output_strategy_sql = f"""
SELECT COUNT(*) as total_rows FROM (
    SELECT t1.account_id
    FROM {t_kuaishou} AS t1
    WHERE (COALESCE(t1.cash_cost::DOUBLE, 0) + COALESCE(t1.credit_cost::DOUBLE, 0) + 
           COALESCE(t1.front_rebate_cost::DOUBLE, 0) + COALESCE(t1.back_rebate_cost::DOUBLE, 0)) > 0.00001
    GROUP BY t1.account_id
)
"""

    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行快手月结数据聚合和关联操作")
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
            'LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)',
            ''
        ).replace(
            'any_value(t2.n2) AS "客户名称",  -- 从媒体账户表获取客户名称',
            'NULL AS "客户名称",  -- account表不存在，使用NULL'
        )
        output_strategy_sql = output_strategy_sql.replace(
            'LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)',
            ''
        )

    # 执行数据汇总
    sql = sql_template.format(kuaishou_table=t_kuaishou)
    execute_sql_with_timing(conn, sql, f"[{entry_name}] 快手数据汇总")

    # 检查最终数据量，决定输出策略
    try:
        conn.execute("SELECT COUNT(*) FROM t_kuaishou_month_final")
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
          "公司名称",
          "结算消耗",
          "账户ID",
          "账户类型",
          "一级行业",
          "二级行业",
          "产品名",
          "用户自选类目",
          "素材打标类目",
          "框返花费(元)",
          "总消耗"
   FROM t_kuaishou_month_final
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
            
            log_success(f"[{entry_name}] 快手月结数据已分离到 {sheets_needed} 个Excel文件，总计 {final_row_count} 行数据")
                
        else:
            # 数据量不大，单个文件单个sheet输出
            export_sql = f"""
-- 导出快手月结数据
COPY
  (SELECT "客户名称",
          "客户编号",
          "公司名称",
          "结算消耗",
          "账户ID",
          "账户类型",
          "一级行业",
          "二级行业",
          "产品名",
          "用户自选类目",
          "素材打标类目",
          "框返花费(元)",
          "总消耗"
   FROM t_kuaishou_month_final) TO '{output_excel_path}' WITH (FORMAT xlsx, HEADER true);
"""
            execute_sql_with_timing(conn, export_sql, f"[{entry_name}] 导出快手月结数据")
            log_success(f"[{entry_name}] 快手月结结果已输出到: {output_excel}")
            
    except Exception as export_e:
        log_error(f"[{entry_name}] 数据导出失败: {export_e}")
        raise
    
    # 最终完成日志
    log_stage("处理完成", f"快手月结数据处理完成，共处理 {final_row_count} 行数据")
