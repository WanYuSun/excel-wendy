import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing


def toutiao_entry_handler(entry_dir: str, excels: List[str],
                          conn: duckdb.DuckDBPyConnection):
    """
    “头条”入口处理函数
    - 查找所需Excel文件（正则匹配），如未找到则提示用户输入文件名
    - 若匹配多个文件，引导用户选择
    - 替换SQL模板并用DuckDB执行
    - 账户表使用已存在的account表，不再创建
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("头条处理", f"开始处理头条入口: {entry_name}")

    # 阶段1: 文件查找和选择
    log_stage("文件查找", f"查找{entry_name}相关文件（消耗、充值、共享钱包）")
    required = [
        (rf'^{re.escape(entry_name)}.*消耗.*\.xlsx$',
         f"未找到{entry_name}消耗文件，请手动输入文件名"),
        (rf'^{re.escape(entry_name)}.*充值.*\.xlsx$',
         f"未找到{entry_name}充值文件，请手动输入文件名"),
        (rf'^{re.escape(entry_name)}.*共享钱包.*\.xlsx$',
         f"未找到{entry_name}共享钱包流水文件，请手动输入文件名"),
    ]
    try:
        found = []
        for i, (pattern, prompt_msg) in enumerate(required):
            matches = [x for x in excels if re.match(pattern, x)]
            f = select_excel_from_matches(matches, entry_dir, prompt_msg)
            found.append(f)
            file_types = ["消耗", "充值", "共享钱包"]
            log_success(f"[{entry_name}] 找到{file_types[i]}文件: {os.path.basename(f)}")
        out_excel, in1_excel, in2_excel = found
    except SkipEntryException as e:
        log_info(f"[{entry_name}] {e}")
        return

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, entry_name)
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # SQL模板，替换为实际路径（不再创建account表，假定已存在）
    sql_template = """
-- 消耗

DROP TABLE IF EXISTS tt1;


CREATE TABLE tt1 AS
SELECT c1 AS "账号ID",
       any_value(c2) AS "账户名称",
       any_value(c4) AS "端口名称",
       sum(c3) AS "消耗"
FROM
  (SELECT "广告主账户id" AS c1,
          "客户名称" AS c2,
          "非赠款消耗"-"返佣消耗" AS c3,
          "一级代理商账户名称" AS c4
   FROM read_xlsx('{out_excel}', sheet='0', empty_as_varchar=true)
   WHERE abs(c3)>0.00001)
GROUP BY ALL;

-- 充值1

DROP TABLE IF EXISTS tt2_1;


CREATE TABLE tt2_1 AS
SELECT c1 AS "账号ID",
       any_value(c2) AS "账户名称",
       any_value(c4) AS "端口名称",
       sum(c3) AS "充值"
FROM
  (SELECT IF("转账类型"='退款', "转出方账户ID", "转入方账户ID") AS c1,
          IF("转账类型"='退款', "转出方客户名称", "转入方客户名称") AS c2,
          "总金额（元）" AS c3,
          "业务平台" AS c4
   FROM read_xlsx('{in1_excel}', sheet='0', empty_as_varchar=true)
   WHERE "转账类型" IN ('退款',
                    '加款')
     AND abs(c3)>0.00001)
GROUP BY ALL;

-- 充值2

DROP TABLE IF EXISTS tt2_2;


CREATE TABLE tt2_2 AS
SELECT c1 AS "账号ID",
       any_value(c2) AS "账户名称",
       any_value(c4) AS "端口名称",
       sum(c3) AS "充值"
FROM
  (SELECT "共享钱包ID" AS c1,
          "共享钱包名称" AS c2,
          "总收入"-"总支出" AS c3,
          "业务线" AS c4
   FROM read_xlsx('{in2_excel}', sheet='0', empty_as_varchar=true)
   WHERE abs(c3)>0.00001)
GROUP BY ALL;

-- 充值1+充值2

DROP TABLE IF EXISTS tt2;


CREATE TABLE tt2 AS
SELECT *
FROM tt2_1
UNION ALL
SELECT *
FROM tt2_2;

-- 消耗+充值

DROP TABLE IF EXISTS tt3;


CREATE TABLE tt3 AS
SELECT COALESCE(k1."账号ID", k2."账号ID") AS "账号ID",
       COALESCE(k1."账户名称", k2."账户名称") AS "账户名称",
       COALESCE(k1."端口名称", k2."端口名称") AS "端口名称",
       COALESCE(k1."消耗", 0.0) AS "消耗",
       COALESCE(k2."充值", 0.0) AS "充值"
FROM tt1 k1
FULL JOIN tt2 k2 ON k1."账号ID"=k2."账号ID"
WHERE abs("消耗")>0.00001
  OR abs("充值")>0.00001 ;

-- 关联账户
COPY
  (SELECT t2.n1 AS "媒体账户主体", t2.n2 AS "客户", t1.*
   FROM tt3 AS t1
   LEFT JOIN account AS t2 ON t1."账号ID"=t2.id) TO '{output_excel}' WITH (FORMAT xlsx,
                                                                         HEADER true);
"""
    # 阶段3: 数据处理和导出
    log_stage("数据处理", "执行头条数据聚合和关联操作")
    out_excel_path = out_excel.replace("\\", "\\\\")
    in1_excel_path = in1_excel.replace("\\", "\\\\")
    in2_excel_path = in2_excel.replace("\\", "\\\\")
    output_excel_path = output_excel.replace("\\", "\\\\")

    log_info(f"[{entry_name}] 消耗文件路径: {out_excel_path}")
    log_info(f"[{entry_name}] 充值文件路径: {in1_excel_path}")
    log_info(f"[{entry_name}] 共享钱包文件路径: {in2_excel_path}")
    log_info(f"[{entry_name}] 输出路径: {output_excel_path}")
    log_info(f"[{entry_name}] 使用已加载的account表进行关联")

    sql = sql_template.format(
        out_excel=out_excel_path,
        in1_excel=in1_excel_path,
        in2_excel=in2_excel_path,
        output_excel=output_excel_path,
    )

    # 阶段4: SQL执行
    log_stage("SQL执行", "执行数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 头条数据处理")
        log_success(f"[{entry_name}] 结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
