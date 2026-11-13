# SQL 支持矩阵

原则：**支持的语法必须正确，不支持的语法必须明确报错**。不做静默容错。

## 支持

### DDL

```sql
CREATE TABLE t (
  id     INT PRIMARY KEY,
  name   VARCHAR(64) NOT NULL,
  age    INT DEFAULT 0,
  active BOOLEAN,
  email  TEXT UNIQUE
);
CREATE TABLE t2 (a INT, b INT, CONSTRAINT uq_ab UNIQUE (a, b));
CREATE TABLE t3 (a INT, b INT, PRIMARY KEY (a, b));
CREATE TABLE t4 (
  a INT CHECK (a > 0),
  b INT CONSTRAINT b_nonneg CHECK (b >= 0),
  CONSTRAINT ck_ab CHECK (a < b)
);
ALTER TABLE t4 ADD CONSTRAINT ck_extra CHECK (a < 100);
ALTER TABLE t4 DROP CONSTRAINT IF EXISTS ck_extra;
CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx_name ON t (col [, col...]);
DROP TABLE [IF EXISTS] t;
DROP INDEX idx_name;
ALTER TABLE t ADD [COLUMN] note TEXT;
ALTER TABLE t ADD [COLUMN] IF NOT EXISTS note TEXT;
ALTER TABLE t ADD COLUMN age INT DEFAULT 18;
ALTER TABLE t ALTER [COLUMN] age SET DEFAULT 21;
ALTER TABLE t ALTER [COLUMN] age DROP DEFAULT;
ALTER TABLE t ALTER [COLUMN] age SET NOT NULL;
ALTER TABLE t ALTER [COLUMN] age DROP NOT NULL;
ALTER TABLE t RENAME TO t2;
ALTER TABLE t2 RENAME [COLUMN] name TO label;
CREATE TABLE t AS SELECT id, name FROM src WHERE age >= 18;
CREATE VIEW v AS SELECT id, name FROM t WHERE age >= 18;
CREATE VIEW v2 (user_id, user_name) AS SELECT id, name FROM t;
CREATE VIEW v3 AS WITH recent AS (SELECT id FROM t WHERE age >= 18) SELECT * FROM recent;
CREATE OR REPLACE VIEW v AS SELECT id FROM t;
DROP VIEW [IF EXISTS] v [CASCADE];
SHOW VIEWS;
DROP TABLE t [CASCADE];
```

`CREATE TABLE AS SELECT` 从查询输出推断列名和类型，只建普通可空列，不自动建
主键或索引；表达式需要显式 `AS` 别名，否则列名可能不是合法标识符（会明确报错）。
来源查询先物化再写入，因此 `CREATE TABLE t AS SELECT * FROM t` 的语义也是确定的。
`IF NOT EXISTS` 遇到同名表会跳过，不会覆盖原表。列级 `UNIQUE` 会为每个非主键
的唯一列建一个唯一索引（`NULL` 不参与唯一性比较）；复合唯一约束请用表级
`UNIQUE (a, b)` 或 `CREATE UNIQUE INDEX`。表级 `PRIMARY KEY (a, b)` 支持复合
主键，参与列自动变为 `NOT NULL`，并生成一个名为 `<table>_pkey`（或显式约束名）
的复合唯一索引。

`CHECK` 约束支持列级和表级声明，也支持 `CONSTRAINT name CHECK (...)` 命名。没有
显式名字时，列级约束自动命名为 `<table>_<column>_check`，表级约束自动命名为
`<table>_check_N`；自动名冲突会追加 `_2`，显式重名或与索引名冲突会在 DDL 阶段
报错。约束表达式以 token 切片持久化到目录，重开数据库后重新解析和绑定，不依赖
进程内缓存。所有 `INSERT`（含 `INSERT ... SELECT`、`DEFAULT VALUES`、`ON CONFLICT`）
以及 `UPDATE`/upsert 写路径都会统一校验；表达式必须返回 `BOOLEAN`（`NULL` 字面量
是 `UNKNOWN`，可通过），结果为 `FALSE` 才拒绝，`NULL`/`UNKNOWN` 视为通过，错误
SQLSTATE 为 `23514`。`ALTER TABLE ... ADD [CONSTRAINT name] CHECK (...)` 会先扫描
存量行，发现违反行时整体失败、不留下约束；`ADD COLUMN` 可以同时声明 CHECK，执行
器会把旧行补上默认值后扫描完整行，失败时不修改目录，默认值本身违反约束也会被
拒绝。带 `CHECK` 的表暂不支持 `RENAME TO` / `RENAME COLUMN`，避免目录里的 token
保留旧列名。`ALTER TABLE ... DROP CONSTRAINT [IF EXISTS] name` 目前只删除 CHECK
约束，只改目录并登记事务 undo，不重写已有行；唯一/主键约束底层是索引，请用
`DROP INDEX` 删除。

`ADD COLUMN` 不重写堆页：目录里增加列定义，旧记录读取时按 `DEFAULT`（没有默认值
则为 `NULL`）补齐，后续 `UPDATE`/`VACUUM` 会把该行写成新格式。`NOT NULL` 新列必须
带非空 `DEFAULT`；主键/唯一约束请先加普通列，再 `ALTER TABLE ... ADD [CONSTRAINT name]
UNIQUE (col...)` / `PRIMARY KEY (col...)` 或 `CREATE UNIQUE INDEX`。`ADD COLUMN IF NOT
EXISTS` 遇到同名列会跳过并返回 notice，不会比较新定义的类型；没有 `IF NOT EXISTS`
时同名列仍报错。`ALTER TABLE ... ADD UNIQUE` 会回填已有数据，
已有重复值时直接报错，不会留下半成品索引。

`ALTER [COLUMN] ... SET DEFAULT` / `DROP DEFAULT` 只修改目录里的默认值，不重写已有行；
后续 `INSERT` 省略该列时读取新默认值，已有行只有在 `UPDATE`/`VACUUM` 重写时才会
重新编码。默认值目前只支持字面常量，且会按列类型做一次转换校验。

`ALTER [COLUMN] ... SET NOT NULL` 会先扫描堆表确认该列没有 `NULL`，校验通过后只修改
目录约束并登记事务 undo；`DROP NOT NULL` 直接解除约束。两者都不重写已有行。主键列
天然要求 `NOT NULL`，因此主键列上的 `DROP NOT NULL` 会被拒绝。

`RENAME TO` 只改目录中的表名，不搬移数据页；`RENAME [COLUMN] old TO new` 只改列名，
同步更新所有索引的列引用，不重写堆记录。重命名后旧名字立即失效；客户端需要重新
解析 SQL（已准备的语句不会自动改写）。表/列名冲突会明确报错。

`CREATE VIEW` 创建只读视图，视图不物化数据，也不占数据页；每次查询时按目录里保存的
定义重新绑定到 `CteScan` 计划。查询视图会读到基表的最新已提交数据，事务/MVCC 语义与
普通查询一致。视图名和表名共享命名空间；视图可以互相引用，但循环引用会被检测并报错。
视图依赖会随目录持久化：`DROP TABLE` 若有视图引用该表会报错，`DROP VIEW` 若有其他
视图引用该视图也会报错，`ALTER TABLE ... RENAME` 同样会拒绝重命名被视图引用的表，
避免留下悬空定义。`CREATE OR REPLACE VIEW` 成功后会重新计算所有视图的依赖快照，
因此替换视图定义后，旧表可以被删除，新表也会正确阻止删除。需要连带删除依赖对象时
使用 `DROP TABLE ... CASCADE` / `DROP VIEW ... CASCADE`；默认仍是 `RESTRICT`。
视图可以带显式列名清单，例如
`CREATE VIEW v (x, y) AS SELECT a, b FROM t`；列名数量
必须与查询输出一致，且列名不能重复，否则在建视图阶段报错。视图定义也支持非递归 `WITH` 和递归 CTE，例如
`CREATE VIEW v AS WITH x AS (SELECT ...) SELECT * FROM x`；
`WITH ... INSERT/UPDATE/DELETE` 不能作为视图定义。视图不能直接
`INSERT`/`UPDATE`/`DELETE`；对视图执行 DML 会得到“表不存在”类错误。
`CREATE OR REPLACE VIEW` 会先完整绑定新定义，成功后再原子替换旧定义；绑定失败时旧视图
保持不变。`DROP VIEW` 只删除目录定义，不影响基表数据；但若其他视图依赖它，会明确报错，
需要先删除依赖视图。`SHOW VIEWS` 列出视图名和保存的定义文本。

### DML

```sql
INSERT INTO t VALUES (1, 'a', 3), (2, 'b', 4);
INSERT INTO t (id, name) VALUES (1, 'a');
INSERT INTO t DEFAULT VALUES;                              -- 按每列默认值写入一行
INSERT INTO t VALUES (1, 'a') ON CONFLICT DO NOTHING;  -- 唯一键冲突时跳过该行
INSERT INTO t VALUES (1, 'a') ON CONFLICT (id)
  DO UPDATE SET name = 'updated';                      -- 冲突时更新已有行
INSERT INTO t (id, name) SELECT id, name FROM src WHERE age >= 18;
UPDATE t SET age = age + 1 WHERE id = 1 RETURNING id, age;
UPDATE t SET age = DEFAULT WHERE id = 1;                  -- 取列当前默认值
DELETE FROM t WHERE age < 0 RETURNING id, name;
WITH old AS (SELECT id FROM t WHERE age < 0) DELETE FROM t
  WHERE id IN (SELECT id FROM old) RETURNING id;
```

`RETURNING` 支持 `*` 和任意表达式，可带 `AS` 别名；INSERT/UPDATE 在**新行**上求值，
DELETE 在**旧行**上求值。绑定阶段就会检查列是否存在，因此未知列的 `RETURNING`
不会先写入再报错。`ON CONFLICT DO NOTHING` 跳过的行不会出现在返回结果里。
`ON CONFLICT DO UPDATE` 会把冲突行更新为新值，`RETURNING` 返回更新后的行；
不带冲突目标时取第一个命中的唯一索引，带 `(col, ...)` 时要求存在完全匹配的
唯一索引；`ON CONSTRAINT name` 按唯一索引名匹配。`DO UPDATE ... WHERE`
的条件在旧行上求值，条件不成立时不更新该行。赋值表达式可以引用旧行当前列，
也可以用 `excluded.col` 读取待插入行。
`WITH` 后面除 SELECT/INSERT 外也接受 UPDATE/DELETE；CTE 只在当前语句内可见。

`INSERT ... SELECT` 的列数必须与目标列一致（有列清单时按列清单算），绑定阶段就会
检查；来源查询先完整物化再写入，所以 `INSERT INTO t SELECT * FROM t` 不会边读边写
导致无限增长。`WITH ... INSERT ... SELECT` 也支持，CTE 只在这次插入的源查询里可见。
`INSERT ... DEFAULT VALUES` 不接受列清单，按目录里的列默认值组装一行；没有默认值的
可空列为 `NULL`，`NOT NULL` 列缺少默认值时按约束报错。它同样支持 `RETURNING` 和
`ON CONFLICT`。

`UPDATE ... SET col = DEFAULT` 与 `ON CONFLICT DO UPDATE SET col = DEFAULT` 在执行时
读取目录里该列的当前默认值，没有默认值时写入 `NULL`；随后仍会经过 `NOT NULL`、唯一
索引等约束检查。它只影响被赋值的列，不会重写其它列。

### 查询

```sql
SELECT [DISTINCT] expr [AS alias], ...
FROM t [alias] [, t2 ...]
  [INNER | LEFT | RIGHT | FULL | CROSS] JOIN t2 ON cond
WHERE cond
GROUP BY expr [, expr...]        -- 列引用或任意标量表达式，同形表达式可直接用于 SELECT
HAVING cond
ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST], ...
LIMIT {n | ALL} [OFFSET m [ROW|ROWS]]  -- n/m 也可以是 $1 这样的参数
OFFSET m [ROW|ROWS]                     -- 可以单独使用
FETCH {FIRST|NEXT} {n | ROW} {ROW|ROWS} {ONLY | WITH TIES}
```

`LIMIT ALL` 等价于不限制行数；`OFFSET m ROWS` 可以单独出现，也可以与
`FETCH FIRST/NEXT ... ROWS` 组合，语义与 `LIMIT n OFFSET m` 一致。
`FETCH FIRST ROW ONLY` 是 `FETCH FIRST 1 ROWS ONLY` 的简写。
`WITH TIES` 在前 n 行之后继续输出与最后一行排序键相同的行；没有 `ORDER BY`
时所有行互为并列，因此会返回偏移之后的所有行。计数仍然受 `OFFSET` 影响。

集合运算：

```sql
SELECT ... UNION [ALL] SELECT ...
SELECT ... EXCEPT [ALL] SELECT ...
SELECT ... INTERSECT [ALL] SELECT ...
SELECT ... UNION ALL SELECT ... ORDER BY 1 LIMIT 5
```

`UNION` 去重（`NULL` 视为同一行），`UNION ALL` 保留重复；`EXCEPT` / `INTERSECT`
按 DISTINCT 语义去重，`EXCEPT ALL` / `INTERSECT ALL` 按多重集计数保留重复。
`INTERSECT` 的优先级高于 `UNION` / `EXCEPT`，同级从左到右结合；列数必须一致、
类型要兼容，输出列名取第一个 `SELECT`。`ORDER BY` / `LIMIT` / `OFFSET` 作用于
整个集合结果，排序键只支持输出列名或从 1 开始的列序号。

公共表表达式（CTE）：

```sql
WITH adults AS (
  SELECT id, name FROM users WHERE age >= 18
), names AS (
  SELECT id, name FROM adults
)
SELECT * FROM names;

WITH c (user_id, user_name) AS (
  SELECT id, name FROM users
)
SELECT user_id, user_name FROM c;

WITH RECURSIVE c (n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM c WHERE n < 5
)
SELECT n FROM c;
```

CTE 按书写顺序绑定：后面的 CTE 和主查询都可以引用前面的 CTE；同名的 CTE 会遮蔽
真实表。`WITH ... INSERT ... SELECT` 同样可用。CTE 支持显式输出列名清单
`WITH c(a, b) AS (...)`，列数必须与查询一致、列名不能重复；递归 CTE 也支持该清单。
`WITH RECURSIVE` 支持 `seed UNION [ALL] recursive` 两部分定义：`UNION ALL` 保留重复，
`UNION` 去重；递归项引用自身时读取上一轮工作表中的行，直到没有新行；超过 10000 轮
会明确报错。不支持把递归 CTE 放在 `WITH ... INSERT/UPDATE/DELETE` 之后。

聚合：`COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX`，参数可以是任意表达式；支持 `COUNT(DISTINCT x)` / `SUM(DISTINCT x)` / `AVG(DISTINCT x)`。

表达式：`+ - * / %`、`||`（字符串拼接，`NULL` 传染）、`= <> < <= > >=`、`AND OR NOT`、`IS NULL`、`IS [NOT] DISTINCT FROM`、`IN (...)`、`BETWEEN a AND b`、`LIKE`（`%` / `_`，支持 `ESCAPE 'c'` 指定单字符转义）、`CAST(x AS type)`、括号、函数 `LENGTH` / `UPPER` / `LOWER` / `ABS` / `COALESCE`。

`LIKE ... ESCAPE 'c'` 用单个字符作为转义符，可把 `%`、`_` 或转义符本身当作普通
字符匹配；转义符为空、长度不是 1，或模式以转义符结尾时会明确报错。没有 `ESCAPE`
时保持原行为，`\` 不是默认转义符。

条件表达式（两种形式都支持）：

```sql
CASE WHEN cond THEN value [WHEN cond THEN value ...] [ELSE value] END
CASE expr WHEN value THEN result [...] [ELSE result] END
```

分支的返回类型必须一致（数值之间会自动提升为 `FLOAT`），否则绑定阶段报
`TypeMismatchError`；`NULL` 字面量作为 `unknown` 类型会被其它分支吸收，
所以 `CASE WHEN ... THEN NULL ELSE 1 END` 的类型是 `INT`。`CASE` 可以出现在聚合参数里，`SUM(CASE WHEN ... THEN 1 ELSE 0 END)`
就是常见的条件计数写法。

子查询：标量子查询、`IN (SELECT ...)`、`EXISTS (SELECT ...)`。
`IN` 子查询必须返回一列，且列类型与左侧操作数兼容（否则在绑定阶段报错，
不会静默返回空结果）。

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
SAVEPOINT name;
ROLLBACK TO [SAVEPOINT] name;  -- 撤销到保存点，事务继续
RELEASE [SAVEPOINT] name;       -- 释放保存点及其之后建立的保存点
EXPLAIN <query>;
EXPLAIN ANALYZE <query>;  -- 真正执行，计划树每行带 actual time / rows / loops
SHOW TABLES;
CHECKPOINT;
REINDEX [TABLE] table;   -- 按堆表当前内容重建该表所有索引
VACUUM [FULL] [table];  -- 回收死版本、压实堆页并重建索引；不带表名则处理所有表
TRUNCATE [TABLE] table;  -- 清空整表（页链保留、逐页重置），索引重建为空
```

## 明确不支持（会报 `NotSupportedError`）

| 语法 | 原因 |
|---|---|
| `ALTER TABLE` 的其它动作（`DROP COLUMN`、改列类型） | 只实现了 `ADD COLUMN` / `ALTER [COLUMN] ... SET/DROP DEFAULT` / `SET/DROP NOT NULL` / `ADD [CONSTRAINT name] CHECK (expr)` / `DROP CONSTRAINT [IF EXISTS] name` / `RENAME TO` / `RENAME [COLUMN] old TO new` |
| `CHECK` 子查询 / 聚合 | 约束表达式必须是行内标量表达式，不能包含 `SELECT` / `WITH` 或聚合函数 |
| 外键约束 | 需要跨表级联与延迟检查 |
| 触发器 / 存储过程 | 未实现 |
| 窗口函数 | 未实现 |
| 事务隔离级别切换 | 内部固定快照隔离 |
| 二进制大对象 / 溢出页 | 行大小硬上限，超限报错而非截断 |
| **相关子查询** | 子查询里引用外层列会报 `ColumnNotFoundError` |
| `ON CONFLICT` 约束目标 | 支持 `DO NOTHING` / `DO UPDATE SET` / `DO UPDATE ... WHERE` / `excluded.col`；列组合或 `ON CONSTRAINT name` 必须映射到一个唯一索引 |
| `HAVING` 里引用 SELECT 别名 | 别名不进入聚合输出 schema；请重复写聚合表达式 |
| 服务端 SQL 级 `PREPARE` / `EXECUTE` | 只支持 wire 协议的 Parse/Bind，不支持 SQL 文本里的 PREPARE |
| autovacuum / 并发 VACUUM | `VACUUM` 要求没有其它活跃事务；不做后台自动触发 |
| 缩小数据文件 | `VACUUM` / `VACUUM FULL` 回收页内空间，但文件大小不缩，空页留在链上 |

## 已知的行为细节

- `SELECT FROM` 这类把关键字当列名的笔误会在解析阶段报错，不会被静默解释成
  "列名叫 FROM"。
- `ORDER BY` 先按输出别名解析，解析不到再按输入列编译成隐藏列。
- `ORDER BY ... NULLS FIRST/LAST` 已实现；默认 `ASC` 时 `NULL` 最后、`DESC` 时
  `NULL` 最前，可使用 `NULLS FIRST` / `NULLS LAST` 逐键显式覆盖。
- `SAVEPOINT` 是事务内保存点：`ROLLBACK TO` 会撤销保存点之后的页写入
  和 DDL/目录对象变更，事务继续保持活动；`RELEASE` 连同它之后建立的
  保存点一起释放。保存点不写 WAL、不跨连接持久化。
- `ON CONFLICT DO NOTHING` 在没有冲突目标时会把任意唯一索引冲突都当作
  "跳过"；写了 `(col, ...)` 目标时只跳过该唯一索引上的冲突，其它唯一索引
  冲突仍会报错。`DO UPDATE` 的目标列必须精确匹配一个唯一索引；
  支持在旧行上求值的 `DO UPDATE ... WHERE`、`excluded.col`，以及
  `ON CONFLICT ON CONSTRAINT 索引名`。
- 单列等值条件与单列范围条件（`>` / `>=` / `<` / `<=` / `BETWEEN`）在列上有索引时
  会走索引；范围谓词仍由上层 `Filter` 复核。`AND` 组合的多个条件暂不合并成索引区间。
- `DISTINCT` 在 `ORDER BY` 之前生效（按输出列去重，隐藏排序键不参与）。
- `LEFT JOIN` 的右侧无匹配时补 `NULL`；`RIGHT JOIN` 的左侧、`FULL JOIN` 的两侧同理。
- `CROSS JOIN` 与逗号连接等价，不写 `ON`。
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
`NULL` 字面量是 `unknown` 类型：它可以被 `UNION` / `CASE` / `COALESCE` 的另一侧
吸收，`SELECT NULL + 1` 也会返回 `NULL` 而不是报类型错误；单独 `SELECT NULL`
或 `CREATE TABLE AS SELECT NULL` 时，输出/落表类型会归一成 `TEXT`。
