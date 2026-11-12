import sys
from pathlib import Path

# 让测试可以直接 `import lumendb`，不必先安装该包
ROOT = Path(__file__).parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
