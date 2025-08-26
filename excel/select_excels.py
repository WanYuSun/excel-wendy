"""
select.py

一个用于从Excel文件中选择数据并使用DuckDB处理的模块化工具。

核心功能:
- 使用DuckDB的read_xlsx方法读取Excel文件
- 支持自定义投影列表
- 支持多个Excel文件的UNION ALL操作
- 所有列读取为varchar类型
- 详细的执行时间记录和彩色日志输出

设计原则:
- 模块化设计，可被其他模块导入使用
- 统一的错误处理和日志记录
- 高性能的SQL执行策略
"""

from typing import List, Optional, Tuple

import duckdb

# 导入统一的日志模块
from excel.log import execute_sql_with_timing, log_error, log_info, log_success


def build_projection_string(projections: List[Tuple[str, str]]) -> str:
    """
    构建投影字符串

    Args:
        projections: 投影列表 [(表达式, 别名), ...]

    Returns:
        投影字符串
    """
    if not projections:
        return "*"

    projection_parts = []
    for expr, alias in projections:
        if alias and alias.strip():
            projection_parts.append(f"{expr} AS {alias}")
        else:
            projection_parts.append(expr)

    return ', '.join(projection_parts)


def select_from_excel(conn: duckdb.DuckDBPyConnection,
                      table_name: str,
                      excel_file_list: List[str],
                      projections: Optional[List[Tuple[str, str]]] = None) -> None:
    """
    从Excel文件中选择数据并创建DuckDB表

    Args:
        conn: DuckDB连接对象
        table_name: 目标表名
        excel_file_list: Excel文件路径列表（具备相同的schema）
        projections: 投影列表 [(表达式, 别名), ...]
                    - None或空列表: 使用 SELECT *
                    - 非空: 使用自定义投影

    功能:
    - 使用DuckDB的read_xlsx方法读取Excel文件
    - 所有列读取为varchar类型（all_varchar=true）
    - 如果输入多个Excel文件，使用UNION ALL合并
    - 记录每个SQL步骤的执行时间
    - 使用彩色日志输出执行状态

    Raises:
        Exception: 操作失败时抛出异常
    """
    if not excel_file_list:
        log_error("Excel文件列表不能为空")
        raise ValueError("Excel文件列表不能为空")

    if not table_name or not table_name.strip():
        log_error("表名不能为空")
        raise ValueError("表名不能为空")

    log_info(f"开始处理 {len(excel_file_list)} 个Excel文件到表: {table_name}")

    try:
        # 确保Excel扩展已加载
        try:
            execute_sql_with_timing(conn, "INSTALL excel", "安装Excel扩展")
            execute_sql_with_timing(conn, "LOAD excel", "加载Excel扩展")
            log_success("Excel扩展加载成功")
        except Exception as e:
            log_info(f"Excel扩展已存在或加载失败: {e}")

        # 构建投影字符串
        projection_str = build_projection_string(projections or [])
        if projections:
            log_info(f"使用自定义投影: SELECT {projection_str}")
        else:
            log_info("使用默认投影: SELECT *")

        # 删除已存在的表
        execute_sql_with_timing(conn, f"DROP TABLE IF EXISTS {table_name}",
                                f"删除已存在的表: {table_name}")

        # 构建SQL查询
        if len(excel_file_list) == 1:
            # 单个文件，直接查询
            excel_file = excel_file_list[0].replace("\\", "\\\\")
            sql = f"""
            CREATE TABLE {table_name} AS
            SELECT {projection_str}
            FROM read_xlsx('{excel_file}', all_varchar=true)
            """
            log_info(f"处理单个Excel文件: {excel_file_list[0]}")

        else:
            # 多个文件，使用UNION ALL
            log_info(f"处理多个Excel文件，使用UNION ALL合并")

            union_queries = []
            for i, excel_file in enumerate(excel_file_list):
                excel_file_escaped = excel_file.replace("\\", "\\\\")
                query = f"SELECT {projection_str} FROM read_xlsx('{excel_file_escaped}', all_varchar=true)"
                union_queries.append(query)
                log_info(f"  添加文件 {i + 1}: {excel_file}")

            # 组合所有查询
            full_union_query = " UNION ALL ".join(union_queries)
            sql = f"CREATE TABLE {table_name} AS ({full_union_query})"

        # 执行SQL
        execute_sql_with_timing(conn, sql, f"创建表 {table_name}")

        # 获取结果统计
        result = execute_sql_with_timing(conn, f"SELECT COUNT(*) FROM {table_name}",
                                         f"统计表 {table_name} 行数")
        total_rows = result.fetchone()[0] if result else 0

        log_success(f"表 {table_name} 创建成功，总行数: {total_rows}")

        # 显示表结构信息
        schema_result = execute_sql_with_timing(conn, f"DESCRIBE {table_name}",
                                                f"获取表 {table_name} 结构信息")
        if schema_result:
            schema_data = schema_result.fetchall()
            log_success(f"表 {table_name} 结构信息:")
            for column_info in schema_data:
                log_info(f"  {column_info[0]}: {column_info[1]}")

    except Exception as e:
        log_error(f"处理Excel文件失败: {str(e)}")
        # 清理可能创建的不完整表
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            log_info(f"已清理不完整的表: {table_name}")
        except:
            pass
        raise


def main():
    """
    测试函数，演示select_from_excel的使用方法
    """
    # 使用统一的日志配置
    from log import setup_logging
    setup_logging()

    # 示例用法
    try:
        # 创建DuckDB连接
        conn = duckdb.connect(":memory:")
        log_info("创建DuckDB内存连接")

        # 示例1: 单个Excel文件，使用默认投影
        excel_files = ["example1.xlsx"]
        select_from_excel(conn, "test_table1", excel_files)

        # 示例2: 多个Excel文件，使用自定义投影
        excel_files = ["example1.xlsx", "example2.xlsx"]
        projections = [("column1", "col1"), ("column2", "col2")]
        select_from_excel(conn, "test_table2", excel_files, projections)

        log_success("所有测试完成")

    except Exception as e:
        log_error(f"测试失败: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
            log_info("DuckDB连接已关闭")


if __name__ == "__main__":
    main()
