#!/bin/bash

# 设置默认的 worker 数量
WORKER_COUNT=${1:-1}

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
if [ $? -ne 0 ]; then
    echo "创建 tmux 会话失败！"
    exit 1
fi

# 启动 coordinator
echo "启动 coordinator..."
tmux send-keys -t $SESSION_NAME:0 "go run mrcoordinator.go pg-*.txt" C-m
if [ $? -ne 0 ]; then
    echo "启动 coordinator 失败！"
    exit 1
fi

# 分割窗口并启动 worker
echo "启动 worker..."
for ((i=1; i<=WORKER_COUNT; i++)); do
    # 垂直分割窗口（左右分割）
    tmux split-window -h -t $SESSION_NAME
    if [ $? -ne 0 ]; then
        echo "分割窗口失败！"
        exit 1
    fi
    # 启动 worker
    tmux send-keys -t $SESSION_NAME:0.$i "go run mrworker.go wc.so" C-m
    if [ $? -ne 0 ]; then
        echo "启动 worker 失败！"
        exit 1
    fi
    # 调整窗口布局
    tmux select-layout -t $SESSION_NAME:0 tiled
    if [ $? -ne 0 ]; then
        echo "调整窗口布局失败！"
        exit 1
    fi
done

# 附加到 tmux 会话
echo "附加到 tmux 会话..."
tmux attach-session -t $SESSION_NAME
if [ $? -ne 0 ]; then
    echo "附加到 tmux 会话失败！"
    exit 1
fi

# 清理操作
echo "使用 'tmux kill-session -t $SESSION_NAME' 关闭所有窗口"