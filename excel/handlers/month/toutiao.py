import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing
from excel.select_excels import select_from_excel


def toutiao_month_entry_handler(entry_dir: str, excels: List[str],
                                conn: duckdb.DuckDBPyConnection):
    """
    "头条"月结入口处理函数
    - 专门处理月结数据，数据量更大，sheet数量更多
    - 查找所需Excel文件（正则匹配），如未找到则提示用户输入文件名
    - 若匹配多个文件，引导用户选择
    - 处理消耗、充值和共享钱包流水三类文件
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("头条月结处理", f"开始处理头条月结入口: {entry_name}")

    # 提取subentry名称
    dash_index = entry_name.find('-')
    if dash_index == -1:
        log_error(f"[{entry_name}] entry_name格式错误，应为entry-subentry格式")
        return
    subentry_name = entry_name[dash_index + 1:]
    log_info(f"[{entry_name}] 子入口名称: {subentry_name}")

    # 阶段1: 文件查找和选择
    log_stage("文件查找", f"查找{entry_name}相关文件")
    try:
        # 查找消耗文件
        consume_pattern = rf'^{re.escape(entry_name)}.*消耗.*\.xlsx$'
        consume_matches = [x for x in excels if re.match(consume_pattern, x)]
        consume_excel = select_excel_from_matches(
            consume_matches, entry_dir,
            f"未找到{entry_name}消耗文件，请手动输入文件名"
        )
        log_success(f"[{entry_name}] 找到消耗文件: {os.path.basename(consume_excel)}")

        # 查找充值文件
        charge_pattern = rf'^{re.escape(entry_name)}.*充值.*\.xlsx$'
        charge_matches = [x for x in excels if re.match(charge_pattern, x)]
        charge_excel = select_excel_from_matches(
            charge_matches, entry_dir,
            f"未找到{entry_name}充值文件，请手动输入文件名"
        )
        log_success(f"[{entry_name}] 找到充值文件: {os.path.basename(charge_excel)}")

        # 查找共享钱包流水文件
        wallet_pattern = rf'^{re.escape(entry_name)}.*共享钱包流水.*\.xlsx$'
        wallet_matches = [x for x in excels if re.match(wallet_pattern, x)]
        wallet_excel = select_excel_from_matches(
            wallet_matches, entry_dir,
            f"未找到{entry_name}共享钱包流水文件，请手动输入文件名"
        )
        log_success(f"[{entry_name}] 找到共享钱包流水文件: {os.path.basename(wallet_excel)}")

    except SkipEntryException as e:
        log_info(f"[{entry_name}] {e}")
        return

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, f"month_{entry_name}")
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载（月结数据处理）
    log_stage("数据加载", "从Excel文件加载头条月结数据到临时表")
    
    from excel.union_sheets import union_sheets_concurrent
    
    # 加载消耗数据
    t_consume = 't_toutiao_consume_month'
    try:
        consume_projections = [
            ('"账户ID"', 'account_id'),
            ('"账户名称"', 'account_name'),
            ('"消耗金额"', 'consume_amount'),
            ('"消耗日期"', 'consume_date')
        ]
        
        union_sheets_concurrent(
            excel_file=consume_excel,
            table_name=t_consume,
            conn=conn,
            projections=consume_projections,
            max_workers=8
        )
        
        log_success(f"[{entry_name}] 消耗数据加载完成")
    except Exception as e:
        log_error(f"[{entry_name}] 消耗数据加载失败: {e}")
        return

    # 加载充值数据
    t_charge = 't_toutiao_charge_month'
    try:
        charge_projections = [
            ('"账户ID"', 'account_id'),
            ('"账户名称"', 'account_name'),
            ('"充值金额"', 'charge_amount'),
            ('"充值日期"', 'charge_date')
        ]
        
        union_sheets_concurrent(
            excel_file=charge_excel,
            table_name=t_charge,
            conn=conn,
            projections=charge_projections,
            max_workers=8
        )
        
        log_success(f"[{entry_name}] 充值数据加载完成")
    except Exception as e:
        log_error(f"[{entry_name}] 充值数据加载失败: {e}")
        return

    # 加载共享钱包流水数据
    t_wallet = 't_toutiao_wallet_month'
    try:
        wallet_projections = [
            ('"账户ID"', 'account_id'),
            ('"账户名称"', 'account_name'),
            ('"流水金额"', 'wallet_amount'),
            ('"流水类型"', 'wallet_type'),
            ('"流水日期"', 'wallet_date')
        ]
        
        union_sheets_concurrent(
            excel_file=wallet_excel,
            table_name=t_wallet,
            conn=conn,
            projections=wallet_projections,
            max_workers=8
        )
        
        log_success(f"[{entry_name}] 共享钱包流水数据加载完成")
    except Exception as e:
        log_error(f"[{entry_name}] 共享钱包流水数据加载失败: {e}")
        return

    # SQL模板，针对头条月结数据优化
    sql_template = """
-- 头条月结数据处理

DROP TABLE IF EXISTS t_toutiao_month;

-- 分别汇总各类数据
WITH consume_summary AS (
    SELECT account_id,
           any_value(account_name) AS account_name,
           sum(consume_amount::DOUBLE) AS total_consume
    FROM {consume_table}
    WHERE consume_amount::DOUBLE > 0.00001
    GROUP BY account_id
),
charge_summary AS (
    SELECT account_id,
           any_value(account_name) AS account_name,
           sum(charge_amount::DOUBLE) AS total_charge
    FROM {charge_table}
    WHERE charge_amount::DOUBLE > 0.00001
    GROUP BY account_id
),
wallet_summary AS (
    SELECT account_id,
           any_value(account_name) AS account_name,
           sum(CASE WHEN wallet_type = '转入' THEN wallet_amount::DOUBLE ELSE 0 END) AS wallet_in,
           sum(CASE WHEN wallet_type = '转出' THEN wallet_amount::DOUBLE ELSE 0 END) AS wallet_out
    FROM {wallet_table}
    WHERE abs(wallet_amount::DOUBLE) > 0.00001
    GROUP BY account_id
)

-- 合并所有数据
CREATE TABLE t_toutiao_month AS
SELECT COALESCE(c.account_id, ch.account_id, w.account_id) AS "账号ID",
       COALESCE(c.account_name, ch.account_name, w.account_name) AS "账户名称",
       COALESCE(total_consume, 0) AS "消耗",
       COALESCE(total_charge, 0) AS "充值", 
       COALESCE(wallet_in, 0) AS "共享钱包转入",
       COALESCE(wallet_out, 0) AS "共享钱包转出"
FROM consume_summary c
FULL OUTER JOIN charge_summary ch ON c.account_id = ch.account_id
FULL OUTER JOIN wallet_summary w ON COALESCE(c.account_id, ch.account_id) = w.account_id;

-- 导出月结数据
COPY
  (SELECT t2.n1 AS "媒体账户主体",
          t2.n2 AS "客户",
          '{subentry_name}' AS "端口名称",
          '月结' AS "数据类型",
          t1.*
   FROM t_toutiao_month AS t1
   LEFT JOIN account AS t2 ON t1."账号ID" = t2.id) TO '{output_excel}' WITH (FORMAT xlsx,
                                                                           HEADER true);
"""

    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行头条月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")

    sql = sql_template.format(
        consume_table=t_consume,
        charge_table=t_charge,
        wallet_table=t_wallet,
        output_excel=output_excel_path,
        subentry_name=subentry_name,
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行头条月结数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 头条月结数据处理")
        log_success(f"[{entry_name}] 头条月结结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
