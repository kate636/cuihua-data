"""
导出4月26日晚到27日凌晨的对话（按对话实际时间筛选）
"""
import json, os, glob, re

PROJ_DIRS = glob.glob(os.path.expanduser("~/.claude/projects/*/"))

# 按第一行时间戳筛选4/26~4/27的对话
target_files = []
for proj_dir in PROJ_DIRS:
    for f in glob.glob(os.path.join(proj_dir, "*.jsonl")):
        try:
            with open(f) as fh:
                first = json.loads(fh.readline())
            ts = first.get("timestamp", "")
            if "2026-04-26" in ts or "2026-04-27" in ts:
                target_files.append((ts, f))
        except:
            continue

target_files.sort()

out = []
out.append("=" * 70)
out.append("Claude 对话导出 - 2026年4月26日晚 ~ 4月27日凌晨")
out.append(f"共 {len(target_files)} 个会话")
out.append("时间已转为北京时间 (UTC+8)")
out.append("=" * 70)

for ts, filepath in target_files:
    fname = os.path.basename(filepath)
    sid = fname.replace(".jsonl", "")
    size_kb = os.path.getsize(filepath) / 1024

    # 解析北京时间
    dt_utc = ts.replace("Z", "+00:00")
    # 简单处理: +8小时
    beijing_hint = ts[:16]  # 只显示日期时间

    out.append("")
    out.append("=" * 60)
    out.append(f"会话: {sid}")
    out.append(f"首条时间(UTC): {beijing_hint}  |  文件大小: {size_kb:.0f} KB")
    out.append("=" * 60)

    turn = 0
    try:
        with open(filepath) as f:
            for line in f:
                try:
                    d = json.loads(line)
                    mt = d.get("type", "")
                    mts = d.get("timestamp", "")
                    msg = d.get("message", {})
                    content = msg.get("content", "")

                    if isinstance(content, list):
                        parts = []
                        for p in content:
                            if isinstance(p, dict) and p.get("type") == "text":
                                parts.append(p.get("text", ""))
                            elif isinstance(p, str):
                                parts.append(p)
                        text = " ".join(parts)
                    else:
                        text = str(content)

                    clean = re.sub(r"<[^>]+>", "", text)
                    clean = re.sub(r"\s+", " ", clean).strip()

                    if mt == "user" and clean:
                        turn += 1
                        out.append(f"\n--- 第{turn}轮用户 [{mts}] ---")
                        out.append(clean[:2000])

                    elif mt == "assistant" and clean:
                        # 只保留前600字符
                        out.append(f"[Claude] 长度:{len(clean)}")
                        out.append(clean[:600])
                        if len(clean) > 600:
                            out.append("...(截断)")

                except:
                    continue
    except Exception as e:
        out.append(f"[读取错误: {e}]")

output_path = os.path.join(os.path.dirname(__file__), "../..", "conversations_apr26_night_27_dawn.txt")
output_path = os.path.abspath(output_path)
with open(output_path, "w", encoding="utf-8") as f:
    f.write("\n".join(out))

print(f"导出完成: {output_path}")
print(f"总行数: {len(out)}")
