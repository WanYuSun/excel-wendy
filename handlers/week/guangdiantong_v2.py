import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing
from excel.select_excels import select_from_excel


def guangdiantong_v2_entry_handler(entry_dir: str, excels: List[str],
                                   conn: duckdb.DuckDBPyConnection):
    """
    “广点通”入口处理函数
    - 查找所需Excel文件（正则匹配），如未找到则提示用户输入文件名
    - 若匹配多个文件，引导用户选择
    - 替换SQL模板并用DuckDB执行
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("广点通处理", f"开始处理广点通入口: {entry_name}")

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
    output_excel = select_output_excel(parent_dir, entry_name)
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载
    log_stage("数据加载", "从Excel文件加载数据到临时表")
    t_g1 = 't_g1'
    try:
        select_from_excel(conn, t_g1, [main_excel],
                          [("COLUMNS([c for c in (*) if c in ['账号ID','账户ID']])", 'c1'),
                           ("COLUMNS([c for c in (*) if c in ['账号名称','账户名称']])", 'c2'),
                           ('"账户总消耗"', 'c3'),
                           ('"总转入(元)"', 'c4'),
                           ('"总转出(元)"', 'c5'),
                           ('"资金类型"', 'c6'),
                           ('"资金账户名称"', 'c7'),
                           ('"所属服务商UID"', 'c8')])
        log_success(f"[{entry_name}] 数据加载完成，表名: {t_g1}")
    except Exception as e:
        log_error(f"[{entry_name}] 数据加载失败: {e}")
        return

    # SQL模板，替换为实际路径
    # 注意：account表现在是独立加载的，不在这里创建
    sql_template = """
-- 消耗+充值

DROP TABLE IF EXISTS t_guang;


CREATE TABLE t_guang AS
SELECT "账号ID",
       any_value("账户名称") AS "账户名称",
       any_value(c8) AS "所属服务商UID",
       sum(m_out) AS "消耗",
       sum(m_in) AS "充值"
FROM
  (SELECT c1 AS "账号ID",
          c2 AS "账户名称",
          c8,
          c3::DOUBLE AS m_out,
          c4::DOUBLE - c5::DOUBLE AS m_in
   FROM {table_name}
   WHERE (abs(m_out) > 0.00001
          OR abs(m_in) > 0.00001)
     AND c6 NOT IN ('虚拟金')
     AND c7 NOT IN ('内部领用金账户',
                    '专用赠送账户'))
GROUP BY ALL;

-- 导出（使用已存在的account表）
COPY
  (SELECT t2.n1 AS "媒体账户主体",
          t2.n2 AS "客户",
          '{subentry_name}' AS "端口名称",
          t1.*
   FROM t_guang AS t1
   LEFT JOIN account AS t2 ON t1."账号ID" = t2.id) TO '{output_excel}' WITH (FORMAT xlsx,
                                                                           HEADER true);
"""
    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")
    log_info(f"[{entry_name}] 输出路径: {output_excel_path}")
    log_info(f"[{entry_name}] 使用已加载的account表进行关联")

    sql = sql_template.format(
        table_name=t_g1,
        output_excel=output_excel_path,
        subentry_name=subentry_name,
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 广点通数据处理")
        log_success(f"[{entry_name}] 结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
