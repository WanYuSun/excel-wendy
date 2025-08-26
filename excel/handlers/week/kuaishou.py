import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing
from excel.select_excels import select_from_excel


def kuaishou_entry_handler(entry_dir: str, excels: List[str],
                           conn: duckdb.DuckDBPyConnection):
    """
    “快手”入口处理函数
    - 查找所需Excel文件（正则匹配），如未找到则提示用户输入文件名
    - 若匹配多个文件，引导用户选择
    - 替换SQL模板并用DuckDB执行
    - 账户表使用已存在的account表，不再创建
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("快手处理", f"开始处理快手入口: {entry_name}")

    # 提取subentry名称（entry-subentry格式）
    dash_index = entry_name.find('-')
    if dash_index == -1:
        log_error(f"[{entry_name}] entry_name格式错误，应为entry-subentry格式")
        return
    subentry_name = entry_name[dash_index + 1:]
    log_info(f"[{entry_name}] 子入口名称: {subentry_name}")

    # 阶段1: 文件查找和选择
    log_stage("文件查找", f"查找{entry_name}充值和消耗文件")
    try:
        # 查找充值文件（单一文件）
        charge_pattern = rf'^{re.escape(entry_name)}.*充值.*\.xlsx$'
        charge_matches = [x for x in excels if re.match(charge_pattern, x)]
        charge_excel = select_excel_from_matches(
            charge_matches, entry_dir,
            f"未找到{entry_name}充值文件，请手动输入文件名"
        )
        log_success(f"[{entry_name}] 找到充值文件: {os.path.basename(charge_excel)}")

        # 查找消耗文件（多个文件，1对N关系）
        consume_pattern = rf'^{re.escape(entry_name)}.*消耗.*\.xlsx$'
        consume_matches = [x for x in excels if re.match(consume_pattern, x)]

        if not consume_matches:
            # 如果没有匹配的消耗文件，提示用户手动输入
            consume_excel = select_excel_from_matches(
                [], entry_dir,
                f"未找到{entry_name}消耗文件，请手动输入文件名"
            )
            consume_excel_list = [consume_excel]
            log_info(f"[{entry_name}] 手动指定消耗文件: {os.path.basename(consume_excel)}")
        else:
            # 将所有匹配的消耗文件转换为绝对路径
            consume_excel_list = [os.path.join(
                entry_dir, f) for f in consume_matches]
            log_success(f"[{entry_name}] 找到 {len(consume_excel_list)} 个消耗文件:")
            for i, file in enumerate(consume_excel_list, 1):
                log_info(f"  {i}. {os.path.basename(file)}")

    except SkipEntryException as e:
        log_info(f"[{entry_name}] {e}")
        return

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, entry_name)
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载
    log_stage("数据加载", "从Excel文件加载充值和消耗数据")
    try:
        charge_table_name = "t_charge"
        consume_table_name = "t_consume"

        log_info(f"[{entry_name}] 开始创建充值表: {charge_table_name}")
        select_from_excel(conn, charge_table_name, [charge_excel], [
            ('"账户ID"', 'c1'),
            ('"企业名称"', 'c2'),
            ('"总金额"', 'c3'),
            ('"激励金额"', 'c4'),
            ('"框返金额"', 'c5'),
            ('"活动框返金额"', 'c6'),
            ('"平台激励金额"', 'c7'),
        ])
        log_success(f"[{entry_name}] 充值表创建完成")

        log_info(f"[{entry_name}] 开始创建消耗表: {consume_table_name}")
        select_from_excel(conn, consume_table_name, consume_excel_list, [
            ('"账户ID"', 'c1'),
            ('"公司名称"', 'c2'),
            ('"总花费(元)"', 'c3'),
            ('"框返花费(元)"', 'c4'),
            ('"激励花费(元)"', 'c5'),
            ('"平台激励花费(元)"', 'c6'),
        ])
        log_success(f"[{entry_name}] 消耗表创建完成")
    except Exception as e:
        log_error(f"[{entry_name}] 数据加载失败: {e}")
        return

    # 创建处理后的消耗表k1
    sql_template = """
-- 处理消耗数据

DROP TABLE IF EXISTS k1;


CREATE TABLE k1 AS
SELECT "账户ID" AS "账号ID",
       any_value("账户名称") AS "账户名称",
       sum(v) AS "消耗"
FROM
  (SELECT c1 AS "账户ID",
          c2 AS "账户名称",
          c3::DOUBLE-c4::DOUBLE-c5::DOUBLE-c6::DOUBLE AS v
   FROM {consume_table_name}
   WHERE abs(v)>0.00001)
GROUP BY ALL;

-- 充值

DROP TABLE IF EXISTS k2;


CREATE TABLE k2 AS
SELECT "账户ID" AS "账号ID",
       any_value("账户名称") AS "账户名称",
       sum(v) AS "充值"
FROM
  (SELECT c1 AS "账户ID",
          c2 AS "账户名称",
          c3::DOUBLE-c4::DOUBLE-c5::DOUBLE-c6::DOUBLE-c7::DOUBLE AS v
   FROM {charge_table_name}
   WHERE abs(v)>0.00001)
GROUP BY ALL;

-- 消耗+充值

DROP TABLE IF EXISTS k3;


CREATE TABLE k3 AS
SELECT COALESCE(k1."账号ID", k2."账号ID") AS "账号ID",
       COALESCE(k1."账户名称", k2."账户名称") AS "账户名称",
       COALESCE(k1."消耗", 0.0) AS "消耗",
       COALESCE(k2."充值", 0.0) AS "充值"
FROM k1
FULL JOIN k2 ON k1."账号ID"=k2."账号ID"
WHERE abs("消耗")>0.00001
  OR abs("充值")>0.00001;

-- 关联账户并导出
COPY
  (SELECT t2.n1 AS "媒体账户主体", t2.n2 AS "客户", '{subentry_name}' AS "端口名称", t1.*
   FROM k3 AS t1
   LEFT JOIN account AS t2 ON t1."账号ID"=t2.id) TO '{output_excel}' WITH (FORMAT xlsx,
                                                                         HEADER true);
"""
    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")
    log_info(f"[{entry_name}] 输出路径: {output_excel_path}")
    log_info(f"[{entry_name}] 使用已加载的account表进行关联")

    sql = sql_template.format(
        charge_table_name=charge_table_name,
        consume_table_name=consume_table_name,
        output_excel=output_excel_path,
        subentry_name=subentry_name,
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 快手数据处理")
        log_success(f"[{entry_name}] 结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
