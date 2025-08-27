# Excel 数据处理程序 - 打包脚本

这个脚本用于分别打包周结和月结两个独立的可执行文件。

## 文件说明

- `excel_operator_week.py` - 专用于周结数据处理
- `excel_operator_month.py` - 专用于月结数据处理

## 打包命令

### 1. 打包周结程序

```bash
pyinstaller --onefile --name excel_operator_week --distpath ./dist --workpath ./build_week excel_operator_week.py
```

### 2. 打包月结程序

```bash
pyinstaller --onefile --name excel_operator_month --distpath ./dist --workpath ./build_month excel_operator_month.py
```

### 3. 一键打包两个程序

在当前目录下运行：

```bash
# Windows
build_both.bat

# macOS/Linux
./build_both.sh
```

## 使用说明

### 周结程序特点：

- 专门处理周结数据
- 数据量相对较小
- 处理速度快
- 生成文件：`excel_week.log`, `excel_week.db`

### 月结程序特点：

- 专门处理月结数据
- 数据量大，使用并发优化
- 支持自动分表处理
- 生成文件：`excel_month.log`, `excel_month.db`

## 输出文件

打包完成后会在 `dist` 目录下生成两个可执行文件：

- `excel_operator_week.exe` (Windows) 或 `excel_operator_week` (macOS/Linux)
- `excel_operator_month.exe` (Windows) 或 `excel_operator_month` (macOS/Linux)

## 部署建议

1. 将对应的可执行文件复制到包含 Excel 数据目录的文件夹中
2. 双击运行对应的程序
3. 按提示操作即可

## 注意事项

- 两个程序使用不同的数据库文件，互不干扰
- 日志文件也分别独立
- 确保数据目录结构符合各自的要求
