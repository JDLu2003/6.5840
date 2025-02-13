#!/bin/bash

# 删除旧的输出文件
echo "删除旧的输出文件..."
rm -f mr-out*

# 编译 word-count 插件
echo "编译 word-count 插件..."
go build -buildmode=plugin ../mrapps/wc.go
if [ $? -ne 0 ]; then
    echo "编译插件失败，请检查代码！"
    exit 1
fi

# 创建 tmux 会话
SESSION_NAME="mr-session"
tmux new-session -d -s $SESSION_NAME

# 启动 coordinator
echo "启动 coordinator..."
tmux send-keys -t $SESSION_NAME:0 "go run mrcoordinator.go pg-*.txt" C-m

# 分割窗口并启动 worker
echo "启动 worker..."
for i in {1}; do
    # 水平分割窗口
    tmux split-window -v -t $SESSION_NAME
    # 启动 worker
    tmux send-keys -t $SESSION_NAME:0.$i "go run mrworker.go wc.so" C-m
    # 调整窗口布局
    tmux select-layout -t $SESSION_NAME:0 tiled
done

# 附加到 tmux 会话
echo "附加到 tmux 会话..."
tmux attach-session -t $SESSION_NAME

echo "use 'tmux kill-session -t mr-session' to close all windows"