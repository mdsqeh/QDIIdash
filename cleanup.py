#!/usr/bin/env python3
"""Clean up US ETF references from templates/index.html"""

with open('templates/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix badge section: remove if/else, keep QDII badge directly
old_badge = (
    '\t\t// Badge\n'
    "\t\tif (state.tab === 'qdii') {\n"
    "\t\t\thtml += `<span class=\"badge badge-qdii\">QDII</span> `;\n"
    "\t\t\thtml += r.market === '场内' ? '<span class=\"badge badge-exchange\">场内</span>' : '<span class=\"badge badge-otc\">场外</span>';\n"
    "\t\t} else {\n"
    "\t\t\thtml += `<span class=\"badge badge-us\">ETF</span>`;\n"
    "\t\t}\n"
)

new_badge = (
    '\t\t// Badge\n'
    "\t\thtml += `<span class=\"badge badge-qdii\">QDII</span> `;\n"
    "\t\thtml += r.market === '场内' ? '<span class=\"badge badge-exchange\">场内</span>' : '<span class=\"badge badge-otc\">场外</span>';\n"
)

if old_badge in content:
    content = content.replace(old_badge, new_badge)
    print('Badge section fixed')
else:
    print('WARNING: badge section not found')
    # Debug: show the actual content
    idx = content.find("// Badge")
    if idx >= 0:
        print(repr(content[idx:idx+500]))

# 2. Fix download filename
content = content.replace(
    "a.download = (state.tab === 'qdii' ? 'QDII' : 'US_ETF') + '_' + new Date().toISOString().slice(0,10) + '.csv';",
    "a.download = 'QDII_' + new Date().toISOString().slice(0,10) + '.csv';"
)

with open('templates/index.html', 'w', encoding='utf-8') as f:
    f.write(content)

# Verify
if 'badge-us' in content:
    print('WARNING: badge-us still present')
if 'US_ETF' in content:
    print('WARNING: US_ETF still present')
if 'tabUs' in content:
    print('WARNING: tabUs still present')
qdii_checks = content.count("state.tab === 'qdii'")
print(f"state.tab === 'qdii' count: {qdii_checks}")
print('Done')
