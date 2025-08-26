import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing
from excel.select_excels import select_from_excel


def guangdiantong_month_entry_handler(entry_dir: str, excels: List[str],
                                      conn: duckdb.DuckDBPyConnection):
    """
    "广点通"月结入口处理函数
    - 专门处理月结数据，数据量更大，sheet数量更多
    - 查找所需Excel文件（正则匹配），如未找到则提示用户输入文件名
    - 若匹配多个文件，引导用户选择
    - 替换SQL模板并用DuckDB执行
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("广点通月结处理", f"开始处理广点通月结入口: {entry_name}")

    # 提取subentry名称（entry-subentry格式）
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

    # 阶段3: 数据加载（月结数据处理，使用并发加载）
    log_stage("数据加载", "从Excel文件加载月结数据到临时表（使用并发处理）")
    t_g1 = 't_g1_month'
    try:
        # 对于月结数据，使用union_sheets_concurrent处理多个sheet
        from excel.union_sheets import union_sheets_concurrent
        
        projections = [
            ('"账号ID"', 'account_id'),
            ('"账户名称"', 'account_name'),
            ('"账户总消耗"', 'total_cost'),
            ('"总转入(元)"', 'total_in'),
            ('"总转出(元)"', 'total_out'),
            ('"资金类型"', 'fund_type'),
            ('"资金账户名称"', 'fund_account_name')
        ]
        
        union_sheets_concurrent(
            excel_file=main_excel,
            table_name=t_g1,
            conn=conn,
            projections=projections,
            max_workers=8  # 月结数据量大，使用更多线程
        )
        
        log_success(f"[{entry_name}] 月结数据加载完成，表名: {t_g1}")
    except Exception as e:
        log_error(f"[{entry_name}] 月结数据加载失败: {e}")
        return

    # SQL模板，针对月结数据优化
    sql_template = """
-- 月结消耗+充值处理

DROP TABLE IF EXISTS t_guang_month;

CREATE TABLE t_guang_month AS
SELECT account_id AS "账号ID",
       any_value(account_name) AS "账户名称",
       sum(m_out) AS "消耗",
       sum(m_in) AS "充值"
FROM
  (SELECT account_id,
          account_name,
          total_cost::DOUBLE AS m_out,
          total_in::DOUBLE - total_out::DOUBLE AS m_in
   FROM {table_name}
   WHERE (abs(total_cost::DOUBLE) > 0.00001
          OR abs(total_in::DOUBLE - total_out::DOUBLE) > 0.00001)
     AND fund_type NOT IN ('虚拟金')
     AND fund_account_name NOT IN ('内部领用金账户',
                                   '专用赠送账户'))
GROUP BY ALL;

-- 导出月结数据（使用已存在的account表）
COPY
  (SELECT t2.n1 AS "媒体账户主体",
          t2.n2 AS "客户",
          '{subentry_name}' AS "端口名称",
          '月结' AS "数据类型",
          t1.*
   FROM t_guang_month AS t1
   LEFT JOIN account AS t2 ON t1."账号ID" = t2.id) TO '{output_excel}' WITH (FORMAT xlsx,
                                                                           HEADER true);
"""
    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")
    log_info(f"[{entry_name}] 输出路径: {output_excel_path}")
    log_info(f"[{entry_name}] 使用已加载的account表进行关联")

    sql = sql_template.format(
        table_name=t_g1,
        output_excel=output_excel_path,
        subentry_name=subentry_name,
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行月结数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 广点通月结数据处理")
        log_success(f"[{entry_name}] 月结结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
