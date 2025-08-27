#!/bin/bash

# Excel数据处理程序 - 一键打包脚本 (macOS/Linux)

echo "==============================================="
echo "    Excel数据处理程序 - 打包脚本"
echo "==============================================="
echo

# 检查是否安装了 pyinstaller
if ! command -v pyinstaller &> /dev/null; then
    echo "❌ PyInstaller 未安装，正在安装..."
    pip install pyinstaller
    if [ $? -ne 0 ]; then
        echo "❌ PyInstaller 安装失败，请手动安装: pip install pyinstaller"
        exit 1
    fi
    echo "✅ PyInstaller 安装成功"
fi

# 创建输出目录
mkdir -p dist
mkdir -p build_week
mkdir -p build_month

echo "🔄 开始打包周结程序..."
pyinstaller --onefile --name excel_operator_week --distpath ./dist --workpath ./build_week excel_operator_week.py

if [ $? -eq 0 ]; then
    echo "✅ 周结程序打包成功: dist/excel_operator_week"
else
    echo "❌ 周结程序打包失败"
    exit 1
fi

echo
echo "🔄 开始打包月结程序..."
pyinstaller --onefile --name excel_operator_month --distpath ./dist --workpath ./build_month excel_operator_month.py

if [ $? -eq 0 ]; then
    echo "✅ 月结程序打包成功: dist/excel_operator_month"
else
    echo "❌ 月结程序打包失败"
    exit 1
fi

echo
echo "==============================================="
echo "🎉 打包完成！"
echo "==============================================="
echo "输出文件位置："
echo "  📁 dist/excel_operator_week    - 周结数据处理程序"
echo "  📁 dist/excel_operator_month   - 月结数据处理程序"
echo
echo "使用方法："
echo "1. 将可执行文件复制到包含Excel数据目录的文件夹中"
echo "2. 双击运行对应的程序"
echo "3. 按提示操作即可"
echo

# 清理临时文件
echo "🧹 清理临时文件..."
rm -rf build_week
rm -rf build_month
rm -f *.spec

echo "✅ 清理完成"
echo
