@echo off
chcp 65001 >nul

REM Excel数据处理程序 - 一键打包脚本 (Windows)

echo ===============================================
echo     Excel数据处理程序 - 打包脚本
echo ===============================================
echo.

REM 检查是否安装了 pyinstaller
pyinstaller --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ PyInstaller 未安装，正在安装...
    pip install pyinstaller
    if %errorlevel% neq 0 (
        echo ❌ PyInstaller 安装失败，请手动安装: pip install pyinstaller
        pause
        exit /b 1
    )
    echo ✅ PyInstaller 安装成功
)

REM 创建输出目录
if not exist dist mkdir dist
if not exist build_week mkdir build_week
if not exist build_month mkdir build_month

echo 🔄 开始打包周结程序...
pyinstaller --onefile --name excel_operator_week --distpath ./dist --workpath ./build_week excel_operator_week.py

if %errorlevel% eq 0 (
    echo ✅ 周结程序打包成功: dist/excel_operator_week.exe
) else (
    echo ❌ 周结程序打包失败
    pause
    exit /b 1
)

echo.
echo 🔄 开始打包月结程序...
pyinstaller --onefile --name excel_operator_month --distpath ./dist --workpath ./build_month excel_operator_month.py

if %errorlevel% eq 0 (
    echo ✅ 月结程序打包成功: dist/excel_operator_month.exe
) else (
    echo ❌ 月结程序打包失败
    pause
    exit /b 1
)

echo.
echo ===============================================
echo 🎉 打包完成！
echo ===============================================
echo 输出文件位置：
echo   📁 dist/excel_operator_week.exe    - 周结数据处理程序
echo   📁 dist/excel_operator_month.exe   - 月结数据处理程序
echo.
echo 使用方法：
echo 1. 将可执行文件复制到包含Excel数据目录的文件夹中
echo 2. 双击运行对应的程序
echo 3. 按提示操作即可
echo.

REM 清理临时文件
echo 🧹 清理临时文件...
if exist build_week rmdir /s /q build_week
if exist build_month rmdir /s /q build_month
if exist *.spec del *.spec

echo ✅ 清理完成
echo.
pause
