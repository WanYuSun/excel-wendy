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
    - 查找所需Excel文件（简化匹配规则，不再需要数字结尾）
    - 只处理消耗数据，不再区分充值和消耗
    - 字段映射：广告主账户id、广告主公司名称、共享子钱包名称、结算消耗、结算一级行业、结算二级行业
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

    # 阶段3: 数据加载（只处理消耗数据）
    log_stage("数据加载", "从Excel文件加载头条月结数据到临时表")
    
    from excel.union_sheets import union_sheets_concurrent
    
    # 头条字段映射：根据图示调整字段结构
    toutiao_projections = [
        ('"广告主账户id"', 'advertiser_account_id'),
        ('"广告主公司名称"', 'advertiser_company_name'),
        ('"共享子钱包名称"', 'shared_wallet_name'),
        ('"结算消耗"', 'settle_consume'),
        ('"结算一级行业"', 'settle_industry_level1'),
        ('"结算二级行业"', 'settle_industry_level2'),
        ('"客户名称"', 'client_name'),
        ('"账户类型"', 'account_type')
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

    # SQL模板，根据图示调整汇总逻辑
    sql_template = """
-- 头条月结数据处理（根据流程图调整汇总逻辑）

DROP TABLE IF EXISTS t_toutiao_month_final;

-- 汇总头条消耗数据，按账户ID和客户名称分组
CREATE TABLE t_toutiao_month_final AS
SELECT advertiser_account_id AS "广告主账户id",
       any_value(advertiser_company_name) AS "广告主公司名称",
       any_value(shared_wallet_name) AS "共享子钱包名称",
       any_value(client_name) AS "客户名称",
       sum(settle_consume::DOUBLE) AS "结算消耗",
       any_value(settle_industry_level1) AS "结算一级行业", 
       any_value(settle_industry_level2) AS "结算二级行业",
       any_value(account_type) AS "账户类型",
       '头条' AS "媒体平台"
FROM {toutiao_table}
WHERE settle_consume::DOUBLE > 0.00001
GROUP BY advertiser_account_id, client_name;

-- 导出月结数据，统一输出格式
COPY
  (SELECT t2.n1 AS "媒体账户主体",
          COALESCE(t1."客户名称", t2.n2) AS "客户",
          t1."媒体平台",
          '月结' AS "数据类型",
          t1."广告主账户id" AS "账户ID",
          t1."广告主公司名称" AS "账户名称",
          t1."共享子钱包名称",
          t1."结算消耗",
          t1."结算一级行业",
          t1."结算二级行业",
          t1."账户类型"
   FROM t_toutiao_month_final AS t1
   LEFT JOIN account AS t2 ON t1."广告主账户id" = t2.id
   ORDER BY t1."结算消耗" DESC) TO '{output_excel}' WITH (FORMAT xlsx, HEADER true);
"""

    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行头条月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")

    sql = sql_template.format(
        toutiao_table=t_toutiao,
        output_excel=output_excel_path
    )

    # 阶段5: SQL执行
    log_stage("SQL执行", "执行头条月结数据处理和导出SQL")
    try:
        execute_sql_with_timing(conn, sql, f"[{entry_name}] 头条月结数据处理")
        log_success(f"[{entry_name}] 头条月结结果已输出到: {output_excel}")
    except Exception as e:
        log_error(f"[{entry_name}] DuckDB执行失败: {e}")
        raise
