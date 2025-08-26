import os
import re
from typing import List

import duckdb

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, execute_sql_with_timing
from excel.select_excels import select_from_excel


def kuaishou_month_entry_handler(entry_dir: str, excels: List[str],
                                 conn: duckdb.DuckDBPyConnection):
    """
    "快手"月结入口处理函数
    - 专门处理月结数据，数据量更大，sheet数量更多
    - 查找所需Excel文件（正则匹配），如未找到则提示用户输入文件名
    - 若匹配多个文件，引导用户选择
    - 替换SQL模板并用DuckDB执行
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("快手月结处理", f"开始处理快手月结入口: {entry_name}")

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
        # 查找充值文件
        charge_pattern = rf'^{re.escape(entry_name)}.*充值.*\.xlsx$'
        charge_matches = [x for x in excels if re.match(charge_pattern, x)]
        charge_excel = select_excel_from_matches(
            charge_matches, entry_dir,
            f"未找到{entry_name}充值文件，请手动输入文件名"
        )
        log_success(f"[{entry_name}] 找到充值文件: {os.path.basename(charge_excel)}")

        # 查找消耗文件
        consume_pattern = rf'^{re.escape(entry_name)}.*消耗.*\.xlsx$'
        consume_matches = [x for x in excels if re.match(consume_pattern, x)]

        if not consume_matches:
            consume_excel = select_excel_from_matches(
                [], entry_dir,
                f"未找到{entry_name}消耗文件，请手动输入文件名"
            )
        else:
            # 对于月结数据，可能有多个消耗文件
            consume_excels = []
            for i, match in enumerate(consume_matches):
                consume_excels.append(os.path.join(entry_dir, match))
                log_info(f"[{entry_name}] 找到消耗文件 {i+1}: {match}")
            
            # 如果只有一个文件，直接使用；如果多个，让用户选择是否全部使用
            if len(consume_matches) == 1:
                consume_excel = consume_excels[0]
            else:
                log_info(f"[{entry_name}] 发现 {len(consume_matches)} 个消耗文件")
                choice = input("是否使用所有消耗文件进行合并处理？(y/n): ").strip().lower()
                if choice in ['y', 'yes', '是']:
                    consume_excel = consume_excels  # 传递文件列表
                else:
                    # 让用户选择单个文件
                    consume_excel = select_excel_from_matches(
                        consume_matches, entry_dir,
                        f"请选择要使用的{entry_name}消耗文件"
                    )

        log_success(f"[{entry_name}] 消耗文件确定完成")

    except SkipEntryException as e:
        log_info(f"[{entry_name}] {e}")
        return

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, f"month_{entry_name}")
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载（月结数据处理）
    log_stage("数据加载", "从Excel文件加载月结数据到临时表")
    
    # 加载充值数据
    t_charge = 't_charge_month'
    try:
        from excel.union_sheets import union_sheets_concurrent
        
        charge_projections = [
            ('"账户ID"', 'account_id'),
            ('"账户名称"', 'account_name'),
            ('"充值金额"', 'charge_amount'),
            ('"充值时间"', 'charge_time')
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

    # 加载消耗数据
    t_consume = 't_consume_month'
    try:
        if isinstance(consume_excel, list):
            # 多个消耗文件，需要合并处理
            log_info(f"[{entry_name}] 处理多个消耗文件合并")
            consume_projections = [
                ('"账户ID"', 'account_id'),
                ('"账户名称"', 'account_name'),
                ('"消耗金额"', 'consume_amount'),
                ('"消耗时间"', 'consume_time')
            ]
            
            # 先处理第一个文件
            union_sheets_concurrent(
                excel_file=consume_excel[0],
                table_name=t_consume,
                conn=conn,
                projections=consume_projections,
                max_workers=8
            )
            
            # 处理其余文件并合并
            for i, file in enumerate(consume_excel[1:], 2):
                temp_table = f't_consume_temp_{i}'
                union_sheets_concurrent(
                    excel_file=file,
                    table_name=temp_table,
                    conn=conn,
                    projections=consume_projections,
                    max_workers=8
                )
                
                # 合并到主表
                execute_sql_with_timing(
                    conn,
                    f"INSERT INTO {t_consume} SELECT * FROM {temp_table}",
                    f"合并消耗文件 {i}"
                )
                
                # 清理临时表
                execute_sql_with_timing(
                    conn,
                    f"DROP TABLE {temp_table}",
                    f"清理临时表 {temp_table}"
                )
        else:
            # 单个消耗文件
            consume_projections = [
                ('"账户ID"', 'account_id'),
                ('"账户名称"', 'account_name'),
                ('"消耗金额"', 'consume_amount'),
                ('"消耗时间"', 'consume_time')
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

    # SQL模板，针对月结数据优化
    sql_template = """
-- 快手月结数据处理

DROP TABLE IF EXISTS t_kuaishou_month;

-- 合并充值和消耗数据
CREATE TABLE t_kuaishou_month AS
SELECT account_id AS "账号ID",
       account_name AS "账户名称", 
       COALESCE(charge_amount, 0) AS "充值",
       COALESCE(consume_amount, 0) AS "消耗"
FROM (
    SELECT account_id,
           any_value(account_name) AS account_name,
           sum(charge_amount::DOUBLE) AS charge_amount
    FROM {charge_table}
    WHERE charge_amount::DOUBLE > 0.00001
    GROUP BY account_id
) t1
FULL OUTER JOIN (
    SELECT account_id,
           any_value(account_name) AS account_name,
           sum(consume_amount::DOUBLE) AS consume_amount
    FROM {consume_table}
    WHERE consume_amount::DOUBLE > 0.00001
    GROUP BY account_id
) t2 ON t1.account_id = t2.account_id;

-- 导出月结数据
COPY
  (SELECT t2.n1 AS "媒体账户主体",
          t2.n2 AS "客户",
          '{subentry_name}' AS "端口名称",
          '月结' AS "数据类型",
          t1.*
   FROM t_kuaishou_month AS t1
   LEFT JOIN account AS t2 ON t1."账号ID" = t2.id) TO '{output_excel}' WITH (FORMAT xlsx,
                                                                           HEADER true);
"""

    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行快手月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")

    sql = sql_template.format(
        charge_table=t_charge,
        consume_table=t_consume,
        output_excel=output_excel_path,
        subentry_name=subentry_name,
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行快手月结数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 快手月结数据处理")
        log_success(f"[{entry_name}] 快手月结结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
