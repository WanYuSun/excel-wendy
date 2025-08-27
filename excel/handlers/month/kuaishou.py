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
    - 查找所需Excel文件（简化匹配规则，不再需要数字结尾）
    - 只处理消耗数据，不再区分充值和消耗
    - 字段映射：账户ID、公司名称、结算消耗、一级行业、二级行业、账户类型
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

    # 阶段3: 数据加载（只处理消耗数据）
    log_stage("数据加载", "从Excel文件加载快手月结数据到临时表")
    
    from excel.union_sheets import union_sheets_concurrent
    
    # 快手字段映射：根据图示调整字段结构
    kuaishou_projections = [
        ('"账户ID"', 'account_id'),
        ('"公司名称"', 'company_name'),
        ('"客户名称"', 'client_name'),
        ('"结算消耗"', 'settle_consume'),
        ('"一级行业"', 'industry_level1'),
        ('"二级行业"', 'industry_level2'),
        ('"账户类型"', 'account_type'),
        ('"钱包名称"', 'wallet_name')
    ]
    
    t_kuaishou = 't_kuaishou_month'
    try:
        # 处理第一个文件
        union_sheets_concurrent(
            excel_file=kuaishou_excels[0],
            table_name=t_kuaishou,
            conn=conn,
            projections=kuaishou_projections,
            max_workers=8
        )
        
        # 处理其余文件并合并
        for i, file in enumerate(kuaishou_excels[1:], 2):
            temp_table = f't_kuaishou_temp_{i}'
            union_sheets_concurrent(
                excel_file=file,
                table_name=temp_table,
                conn=conn,
                projections=kuaishou_projections,
                max_workers=8
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
        
        log_success(f"[{entry_name}] 快手数据加载完成")
    except Exception as e:
        log_error(f"[{entry_name}] 快手数据加载失败: {e}")
        return

    # SQL模板，根据图示调整汇总逻辑
    sql_template = """
-- 快手月结数据处理（根据流程图调整汇总逻辑）

DROP TABLE IF EXISTS t_kuaishou_month_final;

-- 汇总快手消耗数据，按账户ID和客户名称分组
CREATE TABLE t_kuaishou_month_final AS
SELECT account_id AS "账户ID",
       any_value(company_name) AS "公司名称",
       any_value(client_name) AS "客户名称",
       sum(settle_consume::DOUBLE) AS "结算消耗",
       any_value(industry_level1) AS "一级行业",
       any_value(industry_level2) AS "二级行业",
       any_value(account_type) AS "账户类型",
       any_value(wallet_name) AS "钱包名称",
       '快手' AS "媒体平台"
FROM {kuaishou_table}
WHERE settle_consume::DOUBLE > 0.00001
GROUP BY account_id, client_name;

-- 导出月结数据，统一输出格式
COPY
  (SELECT t2.n1 AS "媒体账户主体",
          COALESCE(t1."客户名称", t2.n2) AS "客户",
          t1."媒体平台",
          '月结' AS "数据类型",
          t1."账户ID",
          t1."公司名称" AS "账户名称",
          t1."钱包名称" AS "共享钱包名称",
          t1."结算消耗",
          t1."一级行业",
          t1."二级行业",
          t1."账户类型"
   FROM t_kuaishou_month_final AS t1
   LEFT JOIN account AS t2 ON t1."账户ID" = t2.id
   ORDER BY t1."结算消耗" DESC) TO '{output_excel}' WITH (FORMAT xlsx, HEADER true);
"""

    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行快手月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")

    sql = sql_template.format(
        kuaishou_table=t_kuaishou,
        output_excel=output_excel_path
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行快手月结数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 快手月结数据处理")
        log_success(f"[{entry_name}] 快手月结结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
