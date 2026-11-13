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
ALTER TABLE t ADD [COLUMN] note TEXT;
ALTER TABLE t ADD COLUMN age INT DEFAULT 18;
CREATE TABLE t AS SELECT id, name FROM src WHERE age >= 18;
```

`CREATE TABLE AS SELECT` 从查询输出推断列名和类型，只建普通可空列，不自动建
主键或索引；表达式需要显式 `AS` 别名，否则列名可能不是合法标识符（会明确报错）。
来源查询先物化再写入，因此 `CREATE TABLE t AS SELECT * FROM t` 的语义也是确定的。
`IF NOT EXISTS` 遇到同名表会跳过，不会覆盖原表。

`ADD COLUMN` 不重写堆页：目录里增加列定义，旧记录读取时按 `DEFAULT`（没有默认值
则为 `NULL`）补齐，后续 `UPDATE`/`VACUUM` 会把该行写成新格式。`NOT NULL` 新列必须
带非空 `DEFAULT`；主键/唯一约束请先加普通列，再 `CREATE UNIQUE INDEX`。

### DML

```sql
INSERT INTO t VALUES (1, 'a', 3), (2, 'b', 4);
INSERT INTO t (id, name) VALUES (1, 'a');
INSERT INTO t VALUES (1, 'a') ON CONFLICT DO NOTHING;  -- 唯一键冲突时跳过该行
INSERT INTO t (id, name) SELECT id, name FROM src WHERE age >= 18;
UPDATE t SET age = age + 1 WHERE id = 1;
DELETE FROM t WHERE age < 0;
```

`INSERT ... SELECT` 的列数必须与目标列一致（有列清单时按列清单算），绑定阶段就会
检查；来源查询先完整物化再写入，所以 `INSERT INTO t SELECT * FROM t` 不会边读边写
导致无限增长。`WITH ... INSERT ... SELECT` 也支持，CTE 只在这次插入的源查询里可见。

### 查询

```sql
SELECT [DISTINCT] expr [AS alias], ...
FROM t [alias] [, t2 ...]
  [INNER | LEFT] JOIN t2 ON cond
WHERE cond
GROUP BY expr [, expr...]        -- 列引用或任意标量表达式，同形表达式可直接用于 SELECT
HAVING cond
ORDER BY expr [ASC|DESC], ...
LIMIT n [OFFSET m]               -- n/m 也可以是 $1 这样的参数
```

集合运算：

```sql
SELECT ... UNION [ALL] SELECT ...
SELECT ... UNION ALL SELECT ... ORDER BY 1 LIMIT 5
```

`UNION` 去重（`NULL` 视为同一行），`UNION ALL` 保留重复；列数必须一致、类型要兼容，
输出列名取第一个 `SELECT`。`ORDER BY` / `LIMIT` / `OFFSET` 作用于整个集合结果，
排序键只支持输出列名或从 1 开始的列序号。`EXCEPT` / `INTERSECT` 仍未实现。

公共表表达式（非递归 CTE）：

```sql
WITH adults AS (
  SELECT id, name FROM users WHERE age >= 18
), names AS (
  SELECT id, name FROM adults
)
SELECT * FROM names;
```

CTE 按书写顺序绑定：后面的 CTE 和主查询都可以引用前面的 CTE；同名的 CTE 会遮蔽
真实表。`WITH ... INSERT ... SELECT` 同样可用。没有实现递归 `WITH RECURSIVE`，
也不支持 `WITH x(a, b) AS (...)` 这种列别名清单。

聚合：`COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX`，参数可以是任意表达式；支持 `COUNT(DISTINCT x)` / `SUM(DISTINCT x)` / `AVG(DISTINCT x)`。

表达式：`+ - * / %`、`||`（字符串拼接，`NULL` 传染）、`= <> < <= > >=`、`AND OR NOT`、`IS NULL`、`IN (...)`、`BETWEEN a AND b`、`LIKE`（`%` / `_`）、`CAST(x AS type)`、括号、函数 `LENGTH` / `UPPER` / `LOWER` / `ABS` / `COALESCE`。

条件表达式（两种形式都支持）：

```sql
CASE WHEN cond THEN value [WHEN cond THEN value ...] [ELSE value] END
CASE expr WHEN value THEN result [...] [ELSE result] END
```

分支的返回类型必须一致（数值之间会自动提升为 `FLOAT`），否则绑定阶段报
`TypeMismatchError`；`CASE` 可以出现在聚合参数里，`SUM(CASE WHEN ... THEN 1 ELSE 0 END)`
就是常见的条件计数写法。

子查询：标量子查询、`IN (SELECT ...)`、`EXISTS (SELECT ...)`。

### 参数占位符

```sql
SELECT * FROM t WHERE id = $1;
INSERT INTO t VALUES ($1, $2);
SELECT * FROM t ORDER BY id LIMIT $1;
```

- 走 PostgreSQL **扩展查询协议**（`Parse` / `Bind` / `Describe` / `Execute`）时，
  客户端在 `Bind` 里给参数赋值；文本格式和二进制格式（INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/TEXT/VARCHAR）
  都支持。
- 客户端没声明类型（OID 0）时按字面量推断（整数 → `INT`，小数 → `FLOAT`，其余 → `TEXT`）。
- 参数在**绑定阶段折成字面量**，所以执行计划里不存在"参数数组"；这也意味着
  没有计划缓存（每次 Bind 都重新绑定）。
- **简单查询（`Q`）里写 `$1` 会报 `42P02 undefined_parameter`**，因为简单查询没有
  带参数值的通道。这不是遗漏，是协议本身的区别。

### 事务与运维

```sql
BEGIN;  START TRANSACTION;
COMMIT; ROLLBACK;
EXPLAIN <query>;
EXPLAIN ANALYZE <query>;  -- 真正执行，计划树每行带 actual time / rows / loops
SHOW TABLES;
CHECKPOINT;
REINDEX table;   -- 按堆表当前内容重建该表所有索引
VACUUM [table];  -- 回收死版本、压实堆页并重建索引；不带表名则处理所有表
TRUNCATE [TABLE] table;  -- 清空整表（页链保留、逐页重置），索引重建为空
```

## 明确不支持（会报 `NotSupportedError`）

| 语法 | 原因 |
|---|---|
| `ALTER TABLE` 的其它动作（DROP/RENAME/改类型） | 只实现了 `ADD COLUMN` |
| 外键约束 | 需要跨表级联与延迟检查 |
| 视图 / 触发器 / 存储过程 | 未实现 |
| 窗口函数 | 未实现 |
| 递归 CTE | 只实现了非递归 `WITH` |
| `EXCEPT` / `INTERSECT` | 只实现了 `UNION` / `UNION ALL`，其余集合运算未实现 |
| `RIGHT JOIN` / `FULL JOIN` / `CROSS JOIN` | 可用 `LEFT JOIN` 或逗号连接等价改写 |
| 事务隔离级别切换 | 内部固定快照隔离 |
| 二进制大对象 / 溢出页 | 行大小硬上限，超限报错而非截断 |
| **相关子查询** | 子查询里引用外层列会报 `ColumnNotFoundError` |
| `ON CONFLICT DO UPDATE` / 指定冲突目标 | 只支持 `ON CONFLICT DO NOTHING`，且按"任意唯一索引冲突"跳过 |
| `HAVING` 里引用 SELECT 别名 | 别名不进入聚合输出 schema；请重复写聚合表达式 |
| 服务端 SQL 级 `PREPARE` / `EXECUTE` | 只支持 wire 协议的 Parse/Bind，不支持 SQL 文本里的 PREPARE |
| autovacuum / 并发 VACUUM | `VACUUM` 要求没有其它活跃事务；不做后台自动触发 |
| `VACUUM FULL` / 缩小数据文件 | 回收页内空间，但文件大小不缩，空页留在链上 |
| `NULLS FIRST/LAST` | `NULL` 固定排在最后 |

## 已知的行为细节

- `SELECT FROM` 这类把关键字当列名的笔误会在解析阶段报错，不会被静默解释成
  "列名叫 FROM"。
- `ORDER BY` 先按输出别名解析，解析不到再按输入列编译成隐藏列。
- 单列等值条件与单列范围条件（`>` / `>=` / `<` / `<=` / `BETWEEN`）在列上有索引时
  会走索引；范围谓词仍由上层 `Filter` 复核。`AND` 组合的多个条件暂不合并成索引区间。
- `DISTINCT` 在 `ORDER BY` 之前生效（按输出列去重，隐藏排序键不参与）。
- `LEFT JOIN` 的右侧无匹配时补 `NULL`。
- `HAVING` 作用在聚合输出行上：分组键与聚合结果之外的原表列不可见。
- `GROUP BY` 表达式按语法树同形匹配：`SELECT id % 2 ... GROUP BY id % 2` 合法；
  `GROUP BY id % 2` 与 `GROUP BY id%2` 视为同一个表达式。
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
