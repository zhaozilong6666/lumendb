"""LumenDB —— 从零实现的关系型数据库引擎。

模块依赖方向（严格单向）：

    server -> plan -> engine -> storage -> pager
                 /\\
              sql

任何下层模块都不允许 import 上层模块。
"""

__version__ = "0.1.0"

__all__ = ["__version__"]
