# SQL 支持矩阵

原则：**支持的语法必须正确，不支持的语法必须明确报错**。不做静默容错。

## 支持

### DDL

```sql
CREATE TABLE t (
  id     INT PRIMARY KEY,
  name   VARCHAR(64) NOT NULL,
  age    INT DEFAULT 0,
  active BOOLEAN
);
CREATE [UNIQUE] INDEX idx_name ON t (col [, col...]);
DROP TABLE [IF EXISTS] t;
DROP INDEX idx_name;
```

### DML

```sql
INSERT INTO t VALUES (1, 'a', 3), (2, 'b', 4);
INSERT INTO t (id, name) VALUES (1, 'a');
UPDATE t SET age = age + 1 WHERE id = 1;
DELETE FROM t WHERE age < 0;
```

### 查询

```sql
SELECT [DISTINCT] expr [AS alias], ...
FROM t [alias] [, t2 ...]
  [INNER | LEFT] JOIN t2 ON cond
WHERE cond
GROUP BY col [, col...]          -- 只接受列引用，表达式分组会明确报错
HAVING cond
ORDER BY expr [ASC|DESC], ...
LIMIT n [OFFSET m]
```

聚合：`COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX`。

表达式：`+ - * / %`、`= <> < <= > >=`、`AND OR NOT`、`IS NULL`、`IN (...)`、`BETWEEN a AND b`、`LIKE`（`%` / `_`）、`CAST(x AS type)`、括号、函数 `LENGTH` / `UPPER` / `LOWER` / `ABS` / `COALESCE`。

条件表达式（两种形式都支持）：

```sql
CASE WHEN cond THEN value [WHEN cond THEN value ...] [ELSE value] END
CASE expr WHEN value THEN result [...] [ELSE result] END
```

分支的返回类型必须一致（数值之间会自动提升为 `FLOAT`），否则绑定阶段报
`TypeMismatchError`；`CASE` 可以出现在聚合参数里，`SUM(CASE WHEN ... THEN 1 ELSE 0 END)`
就是常见的条件计数写法。

子查询：标量子查询、`IN (SELECT ...)`、`EXISTS (SELECT ...)`。

### 事务与运维

```sql
BEGIN;  START TRANSACTION;
COMMIT; ROLLBACK;
EXPLAIN <query>;
SHOW TABLES;
CHECKPOINT;
REINDEX table;   -- 按堆表当前内容重建该表所有索引
```

## 明确不支持（会报 `NotSupportedError`）

| 语法 | 原因 |
|---|---|
| `ALTER TABLE` | 需要行格式迁移与目录版本化，未实现 |
| 外键约束 | 需要跨表级联与延迟检查 |
| 视图 / 触发器 / 存储过程 | 未实现 |
| 窗口函数 | 未实现 |
| 递归 CTE / 普通 CTE（`WITH`） | 未实现 |
| `UNION` / `EXCEPT` / `INTERSECT` | 未实现集合运算 |
| `RIGHT JOIN` / `FULL JOIN` / `CROSS JOIN` | 可用 `LEFT JOIN` 或逗号连接等价改写 |
| 事务隔离级别切换 | 内部固定快照隔离 |
| 二进制大对象 / 溢出页 | 行大小硬上限，超限报错而非截断 |
| **相关子查询** | 子查询里引用外层列会报 `ColumnNotFoundError` |
| **`GROUP BY <表达式>`** | 只接受列引用；表达式分组未实现 |
| **`COUNT(DISTINCT x)` 等聚合去重** | 未实现；可用 `SELECT DISTINCT` 先降维 |
| **`PREPARE` 参数占位符 `$1`** | wire 协议里的参数绑定未实现 |
| `NULLS FIRST/LAST` | `NULL` 固定排在最后 |

## 已知的行为细节

- `SELECT FROM` 这类把关键字当列名的笔误会在解析阶段报错，不会被静默解释成
  "列名叫 FROM"。
- `ORDER BY` 先按输出别名解析，解析不到再按输入列编译成隐藏列。
- `DISTINCT` 在 `ORDER BY` 之前生效（按输出列去重，隐藏排序键不参与）。
- `LEFT JOIN` 的右侧无匹配时补 `NULL`。
- `COUNT(*)` 在没有 `GROUP BY` 且输入为空时返回 `0`，`SUM` 返回 `NULL`。
- 除以 0 报错而不是返回 `NULL`。
- `UPDATE` 的赋值表达式在**旧行**上求值（`SET a = a + 1` 语义正确）。

## 类型

| 类型 | 范围 / 说明 |
|---|---|
| `INT` | 32 位有符号 |
| `BIGINT` | 64 位有符号 |
| `VARCHAR(n)` | n 为字符数上限，按 UTF-8 字节存储 |
| `TEXT` | 不限长度（受页大小限制） |
| `BOOLEAN` | `TRUE` / `FALSE` |
| `FLOAT` | IEEE754 double |

隐式转换只允许 `INT → BIGINT`、`INT → FLOAT`。其余需要显式 `CAST`，否则报类型错误。
