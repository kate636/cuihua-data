"""
导出April 26-27的Claude聊天记录为文本文件
"""
import json
import os
import glob
from datetime import datetime

PROJECT_DIR = os.path.expanduser("~/.claude/projects/-Users-zhukate-Desktop-Projects-qdm-fm-----")

# 4月26日和27日的文件
target_files = []
for f in sorted(glob.glob(os.path.join(PROJECT_DIR, "*.jsonl"))):
    fname = os.path.basename(f)
    mtime = os.path.getmtime(f)
    dt = datetime.fromtimestamp(mtime)
    if dt.month == 4 and dt.day in [26, 27]:
        target_files.append((f, dt))

target_files.sort(key=lambda x: x[1])

print(f"找到 {len(target_files)} 个文件")

output_lines = []
output_lines.append("=" * 70)
output_lines.append("Claude 聊天记录导出")
output_lines.append(f"日期范围: 2026年4月26日 ~ 4月27日")
output_lines.append(f"项目: 翠花数据 (fm_etl_v3)")
output_lines.append("=" * 70)

for filepath, mtime in target_files:
    fname = os.path.basename(filepath)
    session_id = fname.replace(".jsonl", "")
    size_kb = os.path.getsize(filepath) / 1024

    output_lines.append("")
    output_lines.append("=" * 70)
    output_lines.append(f"会话: {session_id}")
    output_lines.append(f"时间: {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
    output_lines.append(f"文件大小: {size_kb:.0f} KB")
    output_lines.append("=" * 70)

    try:
        with open(filepath, 'r') as f:
            turn_num = 0
            for line in f:
                try:
                    data = json.loads(line)
                    msg_type = data.get('type', '')
                    message = data.get('message', {})
                    content = message.get('content', '')

                    if msg_type == 'user' and content:
                        turn_num += 1
                        # 清理content中的文本
                        if isinstance(content, list):
                            text_parts = []
                            for part in content:
                                if isinstance(part, dict) and part.get('type') == 'text':
                                    text_parts.append(part.get('text', ''))
                                elif isinstance(part, str):
                                    text_parts.append(part)
                            text = ' '.join(text_parts)
                        else:
                            text = str(content)

                        if text.strip():
                            output_lines.append("")
                            output_lines.append(f"[用户 #{turn_num}]")
                            output_lines.append(text[:5000])  # 截断过长内容

                    elif msg_type == 'assistant' and content:
                        if isinstance(content, list):
                            text_parts = []
                            for part in content:
                                if isinstance(part, dict) and part.get('type') == 'text':
                                    text_parts.append(part.get('text', ''))
                                elif isinstance(part, str):
                                    text_parts.append(part)
                            text = ' '.join(text_parts)
                        else:
                            text = str(content)

                        if text.strip():
                            tool_count = text.count('"name"')
                            output_lines.append(f"[Claude 回复 #{turn_num}]")
                            output_lines.append(text[:5000])
                            output_lines.append(f"  (回复长度: {len(text)} 字符)")

                except json.JSONDecodeError:
                    continue
    except Exception as e:
        output_lines.append(f"[读取错误: {e}]")

# 写入文件
output_path = os.path.join(os.path.dirname(__file__), "..", "..", "conversations_apr26_27.txt")
output_path = os.path.abspath(output_path)
with open(output_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(output_lines))

print(f"导出完成: {output_path}")
print(f"总行数: {len(output_lines)}")
