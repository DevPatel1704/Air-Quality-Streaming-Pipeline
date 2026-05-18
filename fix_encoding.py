import re

header = (
    "# -*- coding: utf-8 -*-\n"
    "import sys, io\n"
    "sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')\n"
    "sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')\n"
)

for fname in ["producer.py", "consumer.py"]:
    with open(fname, "r", encoding="utf-8") as f:
        content = f.read()
    # Strip all non-ASCII characters (emojis etc.)
    fixed = re.sub(r"[^\x00-\x7F]+", "", content)
    if "sys.stdout = io.TextIOWrapper" not in fixed:
        fixed = header + fixed
    with open(fname, "w", encoding="utf-8") as f:
        f.write(fixed)
    print("Fixed: " + fname)

print("Done.")
