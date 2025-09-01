"""
综媒月结数据处理模块

处理综合媒体相关数据，包括小红书、汇川等多种媒体平台
与媒体账户表进行关联，输出完整的月结报告

支持的媒体平台：
- 小红书：小红书*.xlsx
- 汇川：汇川*.xlsx、汇川-小牛*.xlsx、汇川_潇牛*.xlsx等变种
- 趣头条：趣头*.xlsx等变种
- 其他综媒：综媒*.xlsx等

字段映射：
- 小红书文件：发生金额(元) → 子账户名称 → 资金类型
- 汇川/综媒文件：客户名称 → 消费 → 账户id → 账户名称 → 平台新客
- 趣头条文件：广告主名称 → 总消费 → 赔付充值 → 广告主id → 广告主账号
- 媒体账户表：账号ID → 客户名称 → 客户编号 → 媒体账户主体

关联条件：
- 小红书：媒体账户表.账号ID = 小红书.子账户名称
- 汇川/综媒：媒体账户表.账号ID = 文件.账户id
- 趣头条：媒体账户表.账号ID = 趣头条.广告主id

输出字段：客户名称, 客户编号, 客户名称_原始, 消费, 账户id, 账户名称, 平台新客, 数据源
"""

import os
import re
import duckdb
from typing import List

from excel.common import select_excel_from_matches, SkipEntryException, select_output_excel
from excel.log import log_stage, log_error, log_info, log_success, log_warning, execute_sql_with_timing
from excel.select_excels import select_from_excel


def zongmei_month_entry_handler(entry_dir: str, excels: List[str],
                                conn: duckdb.DuckDBPyConnection):
    """
    "综媒"月结入口处理函数
    - 专门处理综媒月结数据，包括小红书、汇川和趣头条
    - 查找小红书相关Excel文件（正则匹配 小红书XXX.xlsx）
    - 查找汇川相关Excel文件（正则匹配 汇川XXX.xlsx）
    - 查找趣头条相关Excel文件（正则匹配 趣头XXX.xlsx）
    - 字段储存：
      * 小红书：发生金额(元)、子账户名称、资金类型
      * 汇川：客户名称、消费、账户id、账户名称、平台新客
      * 趣头条：广告主名称、总消费、赔付充值、广告主id、广告主账号
    - 与媒体账户表联合：
      * 小红书中的子账户名称 对应 媒体账户表的账号ID
      * 汇川中的账户id 对应 媒体账户表的账号ID
      * 趣头条中的广告主id 对应 媒体账户表的账号ID
    - 最终输出：客户名称、客户编号、广告主名称/客户名称_原始、结算消耗/消费、广告主id/账户id、广告主账号/账户名称
    """
    entry_name = os.path.basename(entry_dir)
    log_stage("综媒月结处理", f"开始处理综媒月结入口: {entry_name}")

    # 阶段1: 文件查找和选择
    log_stage("文件查找", f"查找{entry_name}相关文件")
    
    # 调试信息：显示所有找到的Excel文件
    log_info(f"[{entry_name}] 目录中的所有Excel文件: {excels}")
    
    # 查找小红书文件（支持多种格式）
    xiaohongshu_pattern = r'小红书.*\.xlsx$'
    xiaohongshu_matches = [x for x in excels if re.search(
        xiaohongshu_pattern, x, re.IGNORECASE)]
    
    # 如果正则匹配失败，尝试简单的关键词匹配
    if not xiaohongshu_matches:
        log_info(f"[{entry_name}] 正则匹配未找到小红书文件，尝试关键词匹配...")
        xiaohongshu_matches = [x for x in excels if '小红书' in x and x.lower().endswith('.xlsx')]
        if xiaohongshu_matches:
            log_info(f"[{entry_name}] 关键词匹配找到小红书文件: {xiaohongshu_matches}")
    
    # 查找汇川文件（支持多种格式：汇川、汇川-小牛、汇川_潇牛等）
    huichuan_patterns = [
        r'汇川.*\.xlsx$',      # 汇川开头的所有文件
        r'.*汇川.*\.xlsx$'     # 包含汇川的所有文件
    ]
    huichuan_matches = []
    for pattern in huichuan_patterns:
        matches = [x for x in excels if re.search(pattern, x, re.IGNORECASE)]
        for match in matches:
            if match not in huichuan_matches:  # 避免重复
                huichuan_matches.append(match)
    
    # 查找趣头条文件（支持多种格式：趣头条、趣头等）
    qutoutiao_patterns = [
        r'趣头.*\.xlsx$',      # 趣头开头的所有文件
        r'.*趣头.*\.xlsx$'     # 包含趣头的所有文件
    ]
    qutoutiao_matches = []
    for pattern in qutoutiao_patterns:
        matches = [x for x in excels if re.search(pattern, x, re.IGNORECASE)]
        for match in matches:
            if (match not in qutoutiao_matches and 
                match not in huichuan_matches and 
                match not in xiaohongshu_matches):  # 避免重复
                qutoutiao_matches.append(match)
    
    # 查找其他综媒相关文件（如果有的话）
    # 可以添加更多媒体平台的匹配规则
    other_media_patterns = [
        r'综媒.*\.xlsx$',
        r'.*综媒.*\.xlsx$'
    ]
    other_media_matches = []
    for pattern in other_media_patterns:
        matches = [x for x in excels if re.search(pattern, x, re.IGNORECASE)]
        for match in matches:
            # 确保不与已匹配的文件重复
            if (match not in huichuan_matches and 
                match not in xiaohongshu_matches and 
                match not in qutoutiao_matches and
                match not in other_media_matches):
                other_media_matches.append(match)
    
    log_info(f"[{entry_name}] 小红书文件匹配结果: {xiaohongshu_matches}")
    log_info(f"[{entry_name}] 汇川文件匹配结果: {huichuan_matches}")
    log_info(f"[{entry_name}] 趣头条文件匹配结果: {qutoutiao_matches}")
    log_info(f"[{entry_name}] 其他综媒文件匹配结果: {other_media_matches}")
    
    # 处理小红书文件
    xiaohongshu_excels = []
    if xiaohongshu_matches:
        log_info(f"[{entry_name}] 发现小红书文件: {xiaohongshu_matches}")
        if len(xiaohongshu_matches) == 1:
            xiaohongshu_excels = [os.path.join(entry_dir, xiaohongshu_matches[0])]
        else:
            log_info(f"[{entry_name}] 发现 {len(xiaohongshu_matches)} 个小红书文件")
            choice = input("是否使用所有小红书文件进行合并处理？(y/n): ").strip().lower()
            if choice in ['y', 'yes', '是']:
                xiaohongshu_excels = [os.path.join(entry_dir, f) for f in xiaohongshu_matches]
            else:
                selected_file = select_excel_from_matches(
                    xiaohongshu_matches, entry_dir, "请选择要使用的小红书文件"
                )
                xiaohongshu_excels = [selected_file]
    
    # 处理汇川文件
    huichuan_excels = []
    if huichuan_matches:
        log_info(f"[{entry_name}] 发现汇川文件: {huichuan_matches}")
        if len(huichuan_matches) == 1:
            huichuan_excels = [os.path.join(entry_dir, huichuan_matches[0])]
        else:
            log_info(f"[{entry_name}] 发现 {len(huichuan_matches)} 个汇川文件")
            choice = input("是否使用所有汇川文件进行合并处理？(y/n): ").strip().lower()
            if choice in ['y', 'yes', '是']:
                huichuan_excels = [os.path.join(entry_dir, f) for f in huichuan_matches]
            else:
                selected_file = select_excel_from_matches(
                    huichuan_matches, entry_dir, "请选择要使用的汇川文件"
                )
                huichuan_excels = [selected_file]
    
    # 处理趣头条文件
    qutoutiao_excels = []
    if qutoutiao_matches:
        log_info(f"[{entry_name}] 发现趣头条文件: {qutoutiao_matches}")
        if len(qutoutiao_matches) == 1:
            qutoutiao_excels = [os.path.join(entry_dir, qutoutiao_matches[0])]
        else:
            log_info(f"[{entry_name}] 发现 {len(qutoutiao_matches)} 个趣头条文件")
            choice = input("是否使用所有趣头条文件进行合并处理？(y/n): ").strip().lower()
            if choice in ['y', 'yes', '是']:
                qutoutiao_excels = [os.path.join(entry_dir, f) for f in qutoutiao_matches]
            else:
                selected_file = select_excel_from_matches(
                    qutoutiao_matches, entry_dir, "请选择要使用的趣头条文件"
                )
                qutoutiao_excels = [selected_file]
    
    # 处理其他综媒文件
    other_media_excels = []
    if other_media_matches:
        log_info(f"[{entry_name}] 发现其他综媒文件: {other_media_matches}")
        if len(other_media_matches) == 1:
            other_media_excels = [os.path.join(entry_dir, other_media_matches[0])]
        else:
            log_info(f"[{entry_name}] 发现 {len(other_media_matches)} 个其他综媒文件")
            choice = input("是否使用所有其他综媒文件进行合并处理？(y/n): ").strip().lower()
            if choice in ['y', 'yes', '是']:
                other_media_excels = [os.path.join(entry_dir, f) for f in other_media_matches]
            else:
                selected_file = select_excel_from_matches(
                    other_media_matches, entry_dir, "请选择要使用的其他综媒文件"
                )
                other_media_excels = [selected_file]
    
    # 检查是否找到任何文件
    total_files = len(xiaohongshu_excels) + len(huichuan_excels) + len(qutoutiao_excels) + len(other_media_excels)
    if total_files == 0:
        log_warning(f"[{entry_name}] 未找到任何综媒相关文件（小红书、汇川、趣头条等）")
        return
    
    log_success(f"[{entry_name}] 文件确定完成 - 小红书: {len(xiaohongshu_excels)}个, 汇川: {len(huichuan_excels)}个, 趣头条: {len(qutoutiao_excels)}个, 其他: {len(other_media_excels)}个")

    # 阶段2: 准备输出路径
    log_stage("输出准备", "准备输出文件路径")
    parent_dir = os.path.dirname(entry_dir)
    output_excel = select_output_excel(parent_dir, f"month_{entry_name}")
    log_info(f"[{entry_name}] 输出文件: {os.path.basename(output_excel)}")

    # 阶段3: 数据加载
    log_stage("数据加载", "从Excel文件加载综媒月结数据到临时表")

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
                        log_success(
                            f"[{entry_name}] 使用{proj_name}成功处理文件 {file_name}，加载 {loaded_rows} 行数据")
                        return projections, proj_name
                    else:
                        log_warning(
                            f"[{entry_name}] {proj_name}处理成功但未加载到数据，尝试下一套映射")
                        # 清理空表
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                except Exception as count_e:
                    log_warning(f"[{entry_name}] 无法检查数据加载情况: {count_e}")
                    return projections, proj_name

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
                    log_warning(
                        f"[{entry_name}] {proj_name}处理失败: {proj_error}")
                    # 清理可能创建的问题表
                    try:
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except:
                        pass
                    continue

        # 所有映射都失败了
        raise Exception(f"所有字段映射都无法处理文件 {file_name}，可能文件为空或字段名完全不匹配")

    # 小红书字段映射：根据需求定义字段
    # 需要存储：发生金额(元)、子账户名称、资金类型
    xiaohongshu_projections = [
        ('"发生金额(元)"', 'amount'),
        ('"子账户名称"', 'sub_account_name'),
        ('"资金类型"', 'fund_type')
    ]

    # 汇川字段映射：根据需求定义字段
    # 需要存储：客户名称、消费、账户id、账户名称、平台新客
    huichuan_projections = [
        ('"客户名称"', 'customer_name'),
        ('"消费"', 'consumption'),
        ('"账户id"', 'account_id'),
        ('"账户名称"', 'account_name'),
        ('"平台新客"', 'platform_new_customer')
    ]

    # 趣头条字段映射：根据需求定义字段
    # 需要存储：广告主名称、总消费、赔付充值、广告主id、广告主账号
    qutoutiao_projections = [
        ('"广告主名称"', 'customer_name'),
        ('"总消费"', 'total_consumption'),
        ('"赔付充值"', 'refund_recharge'),
        ('"广告主id"', 'account_id'),
        ('"广告主账号"', 'account_name')
    ]

    # 通用综媒字段映射（适用于其他媒体平台）
    # 这个映射会尝试多种可能的字段名组合
    general_media_projections_v1 = [
        ('"客户名称"', 'customer_name'),
        ('"消费"', 'consumption'),
        ('"账户id"', 'account_id'),
        ('"账户名称"', 'account_name'),
        ('"平台新客"', 'platform_new_customer')
    ]
    
    general_media_projections_v2 = [
        ('"客户名称"', 'customer_name'),
        ('"花费"', 'consumption'),
        ('"账户ID"', 'account_id'),
        ('"账户名称"', 'account_name'),
        ('"新客"', 'platform_new_customer')
    ]
    
    general_media_projections_v3 = [
        ('"客户名称"', 'customer_name'),
        ('"成本"', 'consumption'),
        ('"账号ID"', 'account_id'),
        ('"账号名称"', 'account_name'),
        ('"平台新客"', 'platform_new_customer')
    ]

    t_xiaohongshu = 't_xiaohongshu_month'
    t_huichuan = 't_huichuan_month'
    t_qutoutiao = 't_qutoutiao_month'
    t_other_media = 't_other_media_month'

    # 准备所有可用的字段映射策略
    xiaohongshu_projection_strategies = [
        (xiaohongshu_projections, "小红书主要字段映射")
    ]
    
    huichuan_projection_strategies = [
        (huichuan_projections, "汇川主要字段映射"),
        (general_media_projections_v1, "汇川通用字段映射v1"),
        (general_media_projections_v2, "汇川通用字段映射v2"),
        (general_media_projections_v3, "汇川通用字段映射v3")
    ]
    
    qutoutiao_projection_strategies = [
        (qutoutiao_projections, "趣头条主要字段映射"),
        (general_media_projections_v1, "趣头条通用字段映射v1"),
        (general_media_projections_v2, "趣头条通用字段映射v2"),
        (general_media_projections_v3, "趣头条通用字段映射v3")
    ]
    
    other_media_projection_strategies = [
        (general_media_projections_v1, "通用综媒字段映射v1"),
        (general_media_projections_v2, "通用综媒字段映射v2"),
        (general_media_projections_v3, "通用综媒字段映射v3"),
        (huichuan_projections, "综媒汇川格式映射"),
        (qutoutiao_projections, "综媒趣头条格式映射")
    ]

    # 处理小红书文件
    xiaohongshu_processed = False
    if xiaohongshu_excels:
        try:
            log_stage("小红书数据处理", "开始处理小红书文件")
            # 处理第一个小红书文件
            log_info(f"[{entry_name}] 开始处理第一个小红书文件: {xiaohongshu_excels[0]}")

            try:
                used_projections, used_projection_name = safe_process_excel_file(
                    xiaohongshu_excels[0], t_xiaohongshu, xiaohongshu_projection_strategies, entry_name
                )
                log_success(f"[{entry_name}] 第一个小红书文件处理完成，使用了{used_projection_name}")
                xiaohongshu_processed = True
            except Exception as first_file_error:
                log_error(f"[{entry_name}] 第一个小红书文件处理完全失败: {first_file_error}")

            # 处理其余小红书文件
            if xiaohongshu_processed and len(xiaohongshu_excels) > 1:
                for i, file in enumerate(xiaohongshu_excels[1:], 2):
                    log_info(f"[{entry_name}] 开始处理第{i}个小红书文件: {os.path.basename(file)}")
                    temp_table = f't_xiaohongshu_temp_{i}'

                    try:
                        safe_process_excel_file(
                            file, temp_table, [(used_projections, used_projection_name)], entry_name
                        )
                        execute_sql_with_timing(
                            conn,
                            f"INSERT INTO {t_xiaohongshu} SELECT * FROM {temp_table}",
                            f"合并小红书文件 {i}"
                        )
                        execute_sql_with_timing(
                            conn,
                            f"DROP TABLE {temp_table}",
                            f"清理临时表 {temp_table}"
                        )
                        log_success(f"[{entry_name}] 第{i}个小红书文件处理完成")
                    except Exception as file_error:
                        log_warning(f"[{entry_name}] 第{i}个小红书文件处理失败，跳过: {file_error}")

            if xiaohongshu_processed:
                conn.execute(f"SELECT COUNT(*) FROM {t_xiaohongshu}")
                row_count = conn.fetchone()[0]
                log_success(f"[{entry_name}] 小红书数据加载完成，共 {row_count} 行数据")

        except Exception as e:
            log_error(f"[{entry_name}] 小红书数据加载失败: {e}")

    # 处理汇川文件
    huichuan_processed = False
    if huichuan_excels:
        try:
            log_stage("汇川数据处理", "开始处理汇川文件")
            # 处理第一个汇川文件
            log_info(f"[{entry_name}] 开始处理第一个汇川文件: {huichuan_excels[0]}")

            try:
                used_projections, used_projection_name = safe_process_excel_file(
                    huichuan_excels[0], t_huichuan, huichuan_projection_strategies, entry_name
                )
                log_success(f"[{entry_name}] 第一个汇川文件处理完成，使用了{used_projection_name}")
                huichuan_processed = True
            except Exception as first_file_error:
                log_error(f"[{entry_name}] 第一个汇川文件处理完全失败: {first_file_error}")

            # 处理其余汇川文件
            if huichuan_processed and len(huichuan_excels) > 1:
                for i, file in enumerate(huichuan_excels[1:], 2):
                    log_info(f"[{entry_name}] 开始处理第{i}个汇川文件: {os.path.basename(file)}")
                    temp_table = f't_huichuan_temp_{i}'

                    try:
                        safe_process_excel_file(
                            file, temp_table, [(used_projections, used_projection_name)], entry_name
                        )
                        execute_sql_with_timing(
                            conn,
                            f"INSERT INTO {t_huichuan} SELECT * FROM {temp_table}",
                            f"合并汇川文件 {i}"
                        )
                        execute_sql_with_timing(
                            conn,
                            f"DROP TABLE {temp_table}",
                            f"清理临时表 {temp_table}"
                        )
                        log_success(f"[{entry_name}] 第{i}个汇川文件处理完成")
                    except Exception as file_error:
                        log_warning(f"[{entry_name}] 第{i}个汇川文件处理失败，跳过: {file_error}")

            if huichuan_processed:
                conn.execute(f"SELECT COUNT(*) FROM {t_huichuan}")
                row_count = conn.fetchone()[0]
                log_success(f"[{entry_name}] 汇川数据加载完成，共 {row_count} 行数据")

        except Exception as e:
            log_error(f"[{entry_name}] 汇川数据加载失败: {e}")

    # 处理趣头条文件
    qutoutiao_processed = False
    if qutoutiao_excels:
        try:
            log_stage("趣头条数据处理", "开始处理趣头条文件")
            # 处理第一个趣头条文件
            log_info(f"[{entry_name}] 开始处理第一个趣头条文件: {qutoutiao_excels[0]}")

            try:
                used_projections, used_projection_name = safe_process_excel_file(
                    qutoutiao_excels[0], t_qutoutiao, qutoutiao_projection_strategies, entry_name
                )
                log_success(f"[{entry_name}] 第一个趣头条文件处理完成，使用了{used_projection_name}")
                qutoutiao_processed = True
            except Exception as first_file_error:
                log_error(f"[{entry_name}] 第一个趣头条文件处理完全失败: {first_file_error}")

            # 处理其余趣头条文件
            if qutoutiao_processed and len(qutoutiao_excels) > 1:
                for i, file in enumerate(qutoutiao_excels[1:], 2):
                    log_info(f"[{entry_name}] 开始处理第{i}个趣头条文件: {os.path.basename(file)}")
                    temp_table = f't_qutoutiao_temp_{i}'

                    try:
                        safe_process_excel_file(
                            file, temp_table, [(used_projections, used_projection_name)], entry_name
                        )
                        execute_sql_with_timing(
                            conn,
                            f"INSERT INTO {t_qutoutiao} SELECT * FROM {temp_table}",
                            f"合并趣头条文件 {i}"
                        )
                        execute_sql_with_timing(
                            conn,
                            f"DROP TABLE {temp_table}",
                            f"清理临时表 {temp_table}"
                        )
                        log_success(f"[{entry_name}] 第{i}个趣头条文件处理完成")
                    except Exception as file_error:
                        log_warning(f"[{entry_name}] 第{i}个趣头条文件处理失败，跳过: {file_error}")

            if qutoutiao_processed:
                conn.execute(f"SELECT COUNT(*) FROM {t_qutoutiao}")
                row_count = conn.fetchone()[0]
                log_success(f"[{entry_name}] 趣头条数据加载完成，共 {row_count} 行数据")

        except Exception as e:
            log_error(f"[{entry_name}] 趣头条数据加载失败: {e}")

    # 处理其他综媒文件
    other_media_processed = False
    if other_media_excels:
        try:
            log_stage("其他综媒数据处理", "开始处理其他综媒文件")
            # 处理第一个其他综媒文件
            log_info(f"[{entry_name}] 开始处理第一个其他综媒文件: {other_media_excels[0]}")

            try:
                used_projections, used_projection_name = safe_process_excel_file(
                    other_media_excels[0], t_other_media, other_media_projection_strategies, entry_name
                )
                log_success(f"[{entry_name}] 第一个其他综媒文件处理完成，使用了{used_projection_name}")
                other_media_processed = True
            except Exception as first_file_error:
                log_error(f"[{entry_name}] 第一个其他综媒文件处理完全失败: {first_file_error}")

            # 处理其余其他综媒文件
            if other_media_processed and len(other_media_excels) > 1:
                for i, file in enumerate(other_media_excels[1:], 2):
                    log_info(f"[{entry_name}] 开始处理第{i}个其他综媒文件: {os.path.basename(file)}")
                    temp_table = f't_other_media_temp_{i}'

                    try:
                        safe_process_excel_file(
                            file, temp_table, [(used_projections, used_projection_name)], entry_name
                        )
                        execute_sql_with_timing(
                            conn,
                            f"INSERT INTO {t_other_media} SELECT * FROM {temp_table}",
                            f"合并其他综媒文件 {i}"
                        )
                        execute_sql_with_timing(
                            conn,
                            f"DROP TABLE {temp_table}",
                            f"清理临时表 {temp_table}"
                        )
                        log_success(f"[{entry_name}] 第{i}个其他综媒文件处理完成")
                    except Exception as file_error:
                        log_warning(f"[{entry_name}] 第{i}个其他综媒文件处理失败，跳过: {file_error}")

            if other_media_processed:
                conn.execute(f"SELECT COUNT(*) FROM {t_other_media}")
                row_count = conn.fetchone()[0]
                log_success(f"[{entry_name}] 其他综媒数据加载完成，共 {row_count} 行数据")

        except Exception as e:
            log_error(f"[{entry_name}] 其他综媒数据加载失败: {e}")

    # 检查是否有任何数据被成功处理
    if not xiaohongshu_processed and not huichuan_processed and not qutoutiao_processed and not other_media_processed:
        log_error(f"[{entry_name}] 所有文件处理失败，无法继续")
        return

    # 统计处理结果
    processed_data_sources = []
    if xiaohongshu_processed:
        processed_data_sources.append("小红书")
    if huichuan_processed:
        processed_data_sources.append("汇川")
    if qutoutiao_processed:
        processed_data_sources.append("趣头条")
    if other_media_processed:
        processed_data_sources.append("其他综媒")
    
    log_info(f"[{entry_name}] 成功处理的数据源: {', '.join(processed_data_sources)}")

    # SQL模板：处理综媒数据与媒体账户表关联
    # 支持汇川和其他综媒数据的合并
    # 输出字段：客户名称、客户编号、客户名称_原始、消费、账户id、账户名称、平台新客
    sql_template = """
-- 综媒月结数据处理：综合处理汇川和其他综媒数据

DROP TABLE IF EXISTS t_zongmei_month_final;

-- 合并所有综媒数据源
DROP TABLE IF EXISTS t_all_media_combined;
CREATE TABLE t_all_media_combined AS
SELECT customer_name, consumption, account_id, account_name, platform_new_customer, '汇川' as data_source
FROM {huichuan_table}
WHERE 1=1 {huichuan_condition}

UNION ALL

SELECT customer_name, consumption, account_id, account_name, platform_new_customer, '其他综媒' as data_source  
FROM {other_media_table}
WHERE 1=1 {other_media_condition};

-- 汇总综媒数据，与媒体账户表关联
CREATE TABLE t_zongmei_month_final AS
SELECT t2.n2 AS "客户名称",  -- 从媒体账户表获取客户名称
       t2.n3 AS "客户编号",  -- 从媒体账户表获取客户编号
       t1.customer_name AS "客户名称_原始",  -- 原始文件中的客户名称
       sum(COALESCE(t1.consumption::DOUBLE, 0)) AS "消费",  -- 消费求和
       t1.account_id AS "账户id",
       any_value(t1.account_name) AS "账户名称",
       any_value(t1.platform_new_customer) AS "平台新客",
       any_value(t1.data_source) AS "数据源"
FROM t_all_media_combined AS t1
LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)  -- 确保数据类型匹配
GROUP BY t1.account_id, t1.customer_name, t2.n2, t2.n3
ORDER BY "消费" DESC;
"""

    # 构建条件和表名
    huichuan_condition = ""
    other_media_condition = ""
    
    if not huichuan_processed:
        huichuan_condition = "AND 1=0"  # 如果没有汇川数据，添加假条件
    if not other_media_processed:
        other_media_condition = "AND 1=0"  # 如果没有其他综媒数据，添加假条件

    # 如果只有一种数据源，简化SQL
    if huichuan_processed and not other_media_processed:
        sql_template = """
-- 综媒月结数据处理：仅汇川数据

DROP TABLE IF EXISTS t_zongmei_month_final;
CREATE TABLE t_zongmei_month_final AS
SELECT t2.n2 AS "客户名称",  -- 从媒体账户表获取客户名称
       t2.n3 AS "客户编号",  -- 从媒体账户表获取客户编号
       t1.customer_name AS "客户名称_原始",  -- 汇川文件中的客户名称
       sum(COALESCE(t1.consumption::DOUBLE, 0)) AS "消费",  -- 消费求和
       t1.account_id AS "账户id",
       any_value(t1.account_name) AS "账户名称",
       any_value(t1.platform_new_customer) AS "平台新客",
       '汇川' AS "数据源"
FROM {huichuan_table} AS t1
LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)
GROUP BY t1.account_id, t1.customer_name, t2.n2, t2.n3
ORDER BY "消费" DESC;
"""
    elif other_media_processed and not huichuan_processed:
        sql_template = """
-- 综媒月结数据处理：仅其他综媒数据

DROP TABLE IF EXISTS t_zongmei_month_final;
CREATE TABLE t_zongmei_month_final AS
SELECT t2.n2 AS "客户名称",  -- 从媒体账户表获取客户名称
       t2.n3 AS "客户编号",  -- 从媒体账户表获取客户编号
       t1.customer_name AS "客户名称_原始",  -- 综媒文件中的客户名称
       sum(COALESCE(t1.consumption::DOUBLE, 0)) AS "消费",  -- 消费求和
       t1.account_id AS "账户id",
       any_value(t1.account_name) AS "账户名称",
       any_value(t1.platform_new_customer) AS "平台新客",
       '其他综媒' AS "数据源"
FROM {other_media_table} AS t1
LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)
GROUP BY t1.account_id, t1.customer_name, t2.n2, t2.n3
ORDER BY "消费" DESC;
"""

    # 阶段4: 数据处理和导出
    log_stage("数据处理", "执行综媒月结数据聚合和关联操作")
    output_excel_path = output_excel.replace("\\", "\\\\")

    # 首先检查account表是否存在
    account_exists = False
    try:
        conn.execute("SELECT COUNT(*) FROM account")
        result = conn.fetchone()[0]
        log_info(f"[{entry_name}] account表包含 {result} 条记录")
        account_exists = True
    except Exception as e:
        log_warning(f"[{entry_name}] account表不存在或无法访问: {e}")
        log_info(f"[{entry_name}] 将不进行客户名称关联，使用空值填充")

    # 处理所有成功加载的综媒数据（小红书、汇川和其他综媒）
    if xiaohongshu_processed or huichuan_processed or other_media_processed:
        # 修改SQL模板以适应account表不存在的情况
        if not account_exists:
            # 为所有可能的SQL模板添加account表不存在的处理
            sql_template = sql_template.replace(
                'LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)',
                ''
            ).replace(
                't2.n2 AS "客户名称",  -- 从媒体账户表获取客户名称',
                'NULL AS "客户名称",  -- account表不存在，使用NULL'
            ).replace(
                't2.n3 AS "客户编号",  -- 从媒体账户表获取客户编号',
                'NULL AS "客户编号",  -- account表不存在，使用NULL'
            ).replace(
                'GROUP BY t1.account_id, t1.customer_name, t2.n2, t2.n3',
                'GROUP BY t1.account_id, t1.customer_name'
            )

    # 处理所有成功加载的综媒数据（小红书、汇川、趣头条和其他综媒）
    if xiaohongshu_processed or huichuan_processed or qutoutiao_processed or other_media_processed:
        # 如果有小红书数据，需要单独处理
        if xiaohongshu_processed:
            # 创建小红书的统一格式表
            xiaohongshu_sql = f"""
-- 处理小红书数据，转换为统一格式
DROP TABLE IF EXISTS t_xiaohongshu_unified;
CREATE TABLE t_xiaohongshu_unified AS
SELECT 
    '小红书' as customer_name,  -- 小红书文件中没有客户名称，使用固定值
    amount::DOUBLE as consumption,  -- 发生金额作为消费
    sub_account_name as account_id,  -- 子账户名称作为账户id
    sub_account_name as account_name,  -- 子账户名称也作为账户名称
    fund_type as platform_new_customer,  -- 资金类型作为平台新客信息
    '小红书' as data_source
FROM {t_xiaohongshu};
"""
            execute_sql_with_timing(conn, xiaohongshu_sql, f"[{entry_name}] 转换小红书数据格式")

        # 如果有趣头条数据，创建统一格式表
        if qutoutiao_processed:
            qutoutiao_sql = f"""
-- 处理趣头条数据，转换为统一格式
DROP TABLE IF EXISTS t_qutoutiao_unified;
CREATE TABLE t_qutoutiao_unified AS
SELECT 
    customer_name,  -- 广告主名称
    (COALESCE(TRY_CAST(total_consumption AS DOUBLE), 0) - COALESCE(TRY_CAST(refund_recharge AS DOUBLE), 0)) as consumption,  -- 结算消耗 = 总消费 - 赔付充值，安全转换
    account_id,  -- 广告主id作为账户id
    account_name,  -- 广告主账号作为账户名称
    NULL as platform_new_customer,  -- 趣头条没有此字段，使用NULL
    '趣头条' as data_source
FROM {t_qutoutiao};
"""
            execute_sql_with_timing(conn, qutoutiao_sql, f"[{entry_name}] 转换趣头条数据格式")

        # 构建合并SQL
        union_parts = []
        
        if huichuan_processed:
            union_parts.append(f"""
SELECT customer_name, consumption, account_id, account_name, platform_new_customer, '汇川' as data_source
FROM {t_huichuan}
""")
        
        if other_media_processed:
            union_parts.append(f"""
SELECT customer_name, consumption, account_id, account_name, platform_new_customer, '其他综媒' as data_source  
FROM {t_other_media}
""")
        
        if xiaohongshu_processed:
            union_parts.append("""
SELECT customer_name, consumption, account_id, account_name, platform_new_customer, data_source
FROM t_xiaohongshu_unified
""")
        
        if qutoutiao_processed:
            union_parts.append("""
SELECT customer_name, consumption, account_id, account_name, platform_new_customer, data_source
FROM t_qutoutiao_unified
""")

        # 创建合并数据的SQL
        if len(union_parts) > 1:
            if account_exists:
                combined_sql = f"""
-- 综媒月结数据处理：合并所有数据源

DROP TABLE IF EXISTS t_zongmei_month_final;
DROP TABLE IF EXISTS t_all_media_combined;

CREATE TABLE t_all_media_combined AS
{" UNION ALL ".join(union_parts)};

-- 汇总综媒数据，与媒体账户表关联
CREATE TABLE t_zongmei_month_final AS
SELECT t2.n2 AS "客户名称",  -- 从媒体账户表获取客户名称
       t2.n3 AS "客户编号",  -- 从媒体账户表获取客户编号
       t1.customer_name AS "客户名称_原始",  -- 原始文件中的客户名称
       sum(COALESCE(t1.consumption::DOUBLE, 0)) AS "消费",  -- 消费求和
       t1.account_id AS "账户id",
       any_value(t1.account_name) AS "账户名称",
       any_value(t1.platform_new_customer) AS "平台新客",
       any_value(t1.data_source) AS "数据源"
FROM t_all_media_combined AS t1
LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)
GROUP BY t1.account_id, t1.customer_name, t2.n2, t2.n3
ORDER BY "消费" DESC;
"""
            else:
                combined_sql = f"""
-- 综媒月结数据处理：合并所有数据源（无account表）

DROP TABLE IF EXISTS t_zongmei_month_final;
DROP TABLE IF EXISTS t_all_media_combined;

CREATE TABLE t_all_media_combined AS
{" UNION ALL ".join(union_parts)};

-- 汇总综媒数据，无媒体账户表关联
CREATE TABLE t_zongmei_month_final AS
SELECT NULL AS "客户名称",  -- account表不存在，使用NULL
       NULL AS "客户编号",  -- account表不存在，使用NULL
       t1.customer_name AS "客户名称_原始",  -- 原始文件中的客户名称
       sum(COALESCE(t1.consumption::DOUBLE, 0)) AS "消费",  -- 消费求和
       t1.account_id AS "账户id",
       any_value(t1.account_name) AS "账户名称",
       any_value(t1.platform_new_customer) AS "平台新客",
       any_value(t1.data_source) AS "数据源"
FROM t_all_media_combined AS t1
GROUP BY t1.account_id, t1.customer_name
ORDER BY "消费" DESC;
"""
        else:
            # 只有一种数据源的情况
            single_source_table = ""
            single_source_name = ""
            
            if huichuan_processed:
                single_source_table = t_huichuan
                single_source_name = "汇川"
            elif other_media_processed:
                single_source_table = t_other_media
                single_source_name = "其他综媒"
            elif xiaohongshu_processed:
                single_source_table = "t_xiaohongshu_unified"
                single_source_name = "小红书"
            elif qutoutiao_processed:
                single_source_table = "t_qutoutiao_unified"
                single_source_name = "趣头条"
            
            if account_exists:
                combined_sql = f"""
-- 综媒月结数据处理：单一数据源({single_source_name})

DROP TABLE IF EXISTS t_zongmei_month_final;
CREATE TABLE t_zongmei_month_final AS
SELECT t2.n2 AS "客户名称",  -- 从媒体账户表获取客户名称
       t2.n3 AS "客户编号",  -- 从媒体账户表获取客户编号
       t1.customer_name AS "客户名称_原始",  -- 原始文件中的客户名称
       sum(COALESCE(t1.consumption::DOUBLE, 0)) AS "消费",  -- 消费求和
       t1.account_id AS "账户id",
       any_value(t1.account_name) AS "账户名称",
       any_value(t1.platform_new_customer) AS "平台新客",
       '{single_source_name}' AS "数据源"
FROM {single_source_table} AS t1
LEFT JOIN account AS t2 ON CAST(t1.account_id AS VARCHAR) = CAST(t2.id AS VARCHAR)
GROUP BY t1.account_id, t1.customer_name, t2.n2, t2.n3
ORDER BY "消费" DESC;
"""
            else:
                combined_sql = f"""
-- 综媒月结数据处理：单一数据源({single_source_name})，无account表

DROP TABLE IF EXISTS t_zongmei_month_final;
CREATE TABLE t_zongmei_month_final AS
SELECT NULL AS "客户名称",  -- account表不存在，使用NULL
       NULL AS "客户编号",  -- account表不存在，使用NULL
       t1.customer_name AS "客户名称_原始",  -- 原始文件中的客户名称
       sum(COALESCE(t1.consumption::DOUBLE, 0)) AS "消费",  -- 消费求和
       t1.account_id AS "账户id",
       any_value(t1.account_name) AS "账户名称",
       any_value(t1.platform_new_customer) AS "平台新客",
       '{single_source_name}' AS "数据源"
FROM {single_source_table} AS t1
GROUP BY t1.account_id, t1.customer_name
ORDER BY "消费" DESC;
"""

        # 执行数据汇总
        execute_sql_with_timing(conn, combined_sql, f"[{entry_name}] 综媒数据汇总")

        # 检查最终数据量，决定输出策略
        try:
            conn.execute("SELECT COUNT(*) FROM t_zongmei_month_final")
            final_row_count = conn.fetchone()[0]
            log_info(f"[{entry_name}] 汇总后数据量: {final_row_count} 行")

            if final_row_count == 0:
                log_warning(f"[{entry_name}] 汇总后没有数据，请检查关联条件和数据质量")
                return

            # 数据量不大，单个文件单个sheet输出
            # 根据是否有多种数据源调整输出列
            total_sources = len(processed_data_sources)
            if total_sources > 1:
                export_columns = """
"客户名称",
"客户编号", 
"客户名称_原始",
"消费",
"账户id",
"账户名称",
"平台新客",
"数据源"
"""
            else:
                export_columns = """
"客户名称",
"客户编号", 
"客户名称_原始",
"消费",
"账户id",
"账户名称",
"平台新客"
"""
                
            export_sql = f"""
-- 导出综媒月结数据
COPY
  (SELECT {export_columns.strip()}
   FROM t_zongmei_month_final) TO '{output_excel_path}' WITH (FORMAT xlsx, HEADER true);
"""
            execute_sql_with_timing(
                conn, export_sql, f"[{entry_name}] 导出综媒月结数据")
            log_success(f"[{entry_name}] 综媒月结结果已输出到: {output_excel}")

        except Exception as export_e:
            log_error(f"[{entry_name}] 数据导出失败: {export_e}")
            raise

        # 最终完成日志
        log_stage("处理完成", f"综媒月结数据处理完成，共处理 {final_row_count} 行数据，涉及数据源: {', '.join(processed_data_sources)}")
    
    else:
        log_warning(f"[{entry_name}] 没有综媒数据被成功处理，无法生成输出文件")
        return
