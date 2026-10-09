@echo off
chcp 65001 >nul

set "msg=添加笔记"
set /p user_msg=请输入提交信息 (直接回车默认: "添加笔记"): 

if not "%user_msg%"=="" set "msg=%user_msg%"

echo.
echo [1/3] 执行 git add .
git add .

echo [2/3] 执行 git commit -m "%msg%"
git commit -m "%msg%"

echo [3/3] 执行 git push
git push

echo.
echo 全部操作已完成！
pause