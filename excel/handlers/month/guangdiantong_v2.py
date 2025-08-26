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
        
        # 大端口数据字段可能更复杂，支持更多字段
        projections = [
            ('"账号ID"', 'account_id'),
            ('"账户名称"', 'account_name'),
            ('"账户总消耗"', 'total_cost'),
            ('"总转入(元)"', 'total_in'),
            ('"总转出(元)"', 'total_out'),
            ('"资金类型"', 'fund_type'),
            ('"资金账户名称"', 'fund_account_name'),
            ('"主体名称"', 'entity_name'),
            ('"客户名称"', 'client_name'),
            ('"业务线"', 'business_line')
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

    # SQL模板，针对大端口月结数据优化
    sql_template = """
-- 广点通大端口月结消耗+充值处理

DROP TABLE IF EXISTS t_guang_v2_month;

-- 数据预处理和清洗
CREATE TABLE t_guang_v2_month AS
SELECT account_id AS "账号ID",
       any_value(account_name) AS "账户名称",
       any_value(entity_name) AS "主体名称",
       any_value(client_name) AS "客户名称", 
       any_value(business_line) AS "业务线",
       sum(m_out) AS "消耗",
       sum(m_in) AS "充值"
FROM
  (SELECT account_id,
          account_name,
          entity_name,
          client_name,
          business_line,
          total_cost::DOUBLE AS m_out,
          total_in::DOUBLE - total_out::DOUBLE AS m_in
   FROM {table_name}
   WHERE (abs(total_cost::DOUBLE) > 0.00001
          OR abs(total_in::DOUBLE - total_out::DOUBLE) > 0.00001)
     AND fund_type NOT IN ('虚拟金', '赠送金')
     AND fund_account_name NOT IN ('内部领用金账户',
                                   '专用赠送账户',
                                   '测试账户'))
GROUP BY ALL;

-- 创建优化索引提升查询性能
CREATE INDEX IF NOT EXISTS idx_account_id ON t_guang_v2_month("账号ID");

-- 导出大端口月结数据（包含更多维度信息）
COPY
  (SELECT t2.n1 AS "媒体账户主体",
          t2.n2 AS "关联客户",
          '{subentry_name}' AS "端口名称",
          '月结-大端口' AS "数据类型",
          t1."主体名称",
          t1."客户名称",
          t1."业务线",
          t1."账号ID",
          t1."账户名称",
          t1."消耗",
          t1."充值"
   FROM t_guang_v2_month AS t1
   LEFT JOIN account AS t2 ON t1."账号ID" = t2.id
   ORDER BY t1."消耗" DESC) TO '{output_excel}' WITH (FORMAT xlsx,
                                                     HEADER true);
"""
    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行大端口月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")
    log_info(f"[{entry_name}] 输出路径: {output_excel_path}")
    log_info(f"[{entry_name}] 使用已加载的account表进行关联")

    sql = sql_template.format(
        table_name=t_g2,
        output_excel=output_excel_path,
        subentry_name=subentry_name,
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行大端口月结数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 广点通大端口月结数据处理")
        log_success(f"[{entry_name}] 大端口月结结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
