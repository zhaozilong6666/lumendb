# 存储格式（字节级）

所有整数小端序（little-endian）。页大小固定 4096 字节。

## 1. 页头（32 字节，堆页与索引页共用）

| 偏移 | 类型 | 字段 | 说明 |
|---|---|---|---|
| 0 | u16 | `page_type` | 0=FREE, 1=HEAP, 2=BTREE_INTERNAL, 3=BTREE_LEAF, 4=META |
| 2 | u16 | `nslots` | 槽数量（仅堆页；索引页此处存 `nkeys`） |
| 4 | u16 | `free_start` | 槽目录后第一个空闲字节（仅堆页） |
| 6 | u16 | `free_end` | 记录区起始（自页尾向上增长，仅堆页） |
| 8 | u32 | `next_page` | 同层链表下一页，`0xFFFFFFFF` 表示结束 |
| 12 | u32 | `prev_page` | 同上，反向 |
| 16 | u32 | `page_id` | 自描述，便于校验错页写入 |
| 20 | u32 | `reserved` | 保留，写 0 |
| 24 | u64 | `lsn` | pageLSN，恢复时判断日志是否已应用；日志偏移可能超过 4GB，故用 64 位 |

## 2. 堆页布局

```
+--------------------------------------------------+  0
| page header (32B)                                |
+--------------------------------------------------+  32
| slot[0] (4B: offset u16, length u16)             |
| slot[1]                                           |
| ...                          free_start →        |
|                                                  |
|          <<< 空闲区 >>>                          |
|                                                  |
|                     ← free_end                   |
| record[n-1]                                      |
| ...                                              |
| record[0]                                        |
+--------------------------------------------------+  4096
```

- `free_space = free_end - free_start`
- 插入记录：先在槽目录追加槽位（`free_start += 4`），再把记录体放在 `free_end - size`。
- 删除记录：槽位置 `length = 0`（tombstone），记录体不回收；`compact()` 重排记录区并修正所有槽位偏移。
- RID = `(page_id u32, slot u16)`，6 字节逻辑地址，**永不复用**。

## 3. 堆记录

记录头 **38 字节**，格式串 `"<QQIHBBIHIHH"`：

```
+0   u64 xmin          创建事务号
+8   u64 xmax          删除事务号（0 表示未删除）
+16  u32 prev_page     版本链前驱页号（0xFFFFFFFF 表示无）
+20  u16 prev_slot
+22  u8  flags         0x01 已删除 / 0x02 根版本 / 0x04 已中止
+23  u8  reserved      对齐保留
+24  u32 root_page     根版本所在页（根自己指向自己）
+28  u16 root_slot
+30  u32 newest_page   版本链上最新的物理版本页号
+34  u16 newest_slot
+36  u16 ncols
+38  bitmap[(ncols+7)//8]   NULL 位图，1 表示该列为 NULL
+..  列数据，按 schema 顺序、紧凑排列
```

`root_page/root_slot` 与 `newest_page/newest_slot` 是"版本链"的核心：

- 索引里存的是**根 RID**，它在行的整个生命周期内不变；
- 每次 `UPDATE` 写一条新版本，`prev_*` 指回旧版本，同时把根记录的
  `newest_*` 改成新版本的位置；
- 扫描时按 `newest_*` 从新往旧找第一个在事务快照下可见的版本。

这样"索引项不随更新而变化"和"读到最新可见版本"两件事可以同时成立。

`VACUUM` 会打破"根 RID 不变"这条不变量：它把每个存活行重写成唯一的根元组
（`root = newest = 自己`、`prev` 为空），因此 **RID 会整体改变**，必须随后
重建索引。回收之后的表不再有版本链，只有每行一条记录，这就是"表被压实"的
含义。

列编码：

| 类型 | 编码 |
|---|---|
| `INT` / `BIGINT` | 有符号 varint（zigzag + 7 位延续位） |
| `FLOAT` | IEEE754 double，8 字节定长 |
| `BOOLEAN` | 1 字节 |
| `VARCHAR(n)` / `TEXT` | varint 长度 + UTF-8 字节 |

行大小上限为 `PAGE_SIZE - 32 - 4`（页头 + 一个槽位），超长报 `RowTooLargeError`
（不做 TOAST / 溢出页，明确拒绝而不是截断）。

## 4. B+ 树页

类型 2 = 内部节点，3 = 叶子节点。两者共用 §1 的 32 字节页头。

```
内部节点（页头之后是交错的 child / key 数组）:
+32 child[0] u32
+36 key[0]  变长（编码后的键，长度由列 schema 决定）
+..  child[1] u32
+..  key[1]
...
语义: child[i] 子树里的键都落在 [key[i-1], key[i]) 区间
（key[i-1] 是子树下界，key[i] 是上界；不存最小键，靠父节点传下的边界）

叶子节点（记录区向上、槽位目录向下，中间是空闲区）:
+0   u16 page_type
+2   u16 nkeys
+4   u16 data_end      ← 记录区末尾（从 32 开始向上增长）
+6   u16 reserved
+8   u32 next_page     ← 叶子链，范围扫描依赖它
+12  u32 prev_page
+16  u32 page_id
+20  u32 reserved
+24  u64 lsn
+32  ...记录区：编码 key + 6 字节 RID，逐条向上追加...
                          ← 空闲区 →
+4096-2(n+1) ... 槽位目录：slot[i] = 第 i 条记录相对页首的偏移（u16）
```

- **槽位目录按 key 逻辑顺序排列**，记录体在页内的物理顺序无所谓。查找时对槽位
  做页内二分（`_leaf_bisect`），命中后只解码命中的那几条记录，不再整页解码。
- `free_space = 4096 - 2·nkeys - data_end`。
- 插入：二分定位 → 槽位搬移（`memmove` 一段 u16）→ 记录追加到 `data_end`。
  空闲不够时先 `compact()` 把存活记录重排紧凑；仍然放不下才分裂。
- 删除：只摘掉槽位（后面的槽位前移 2 字节），记录体留在原地成为碎片；
  `compact()` 才真正回收。这是用"删除变快"换"页内碎片"，代价由插入时的
  compact 兜住。
- 分裂：按 `n // 2` 分成两页，右页的第一条键上升到父节点；左右页的
  `next_page` / `prev_page` 同时修补。
- **不做借位与合并**：节点键数为 0 时才从父节点摘除，父节点稀疏不处理；
  根节点只剩一个孩子时降低树高（把孩子的字节搬进根页）。长期大量删除后
  索引会变稀疏，用 `REINDEX table` 重建回收。

key 编码与列类型一致（INT 用 zigzag varint，VARCHAR 用 len+bytes）。叶子链保证范围扫描只需顺序读叶页，且 `check_invariants()` 会校验"链上顺序 == 树的中序顺序"。

## 5. WAL 记录

```
+0  u64 lsn
+8  u32 xid
+12 u8  type
+13 u32 payload_len
+17 payload
```

| type | payload |
|---|---|
| `BEGIN=1` | 空 |
| `COMMIT=2` | 空 |
| `ABORT=3` | 空 |
| `UPDATE=4` | `u32 page_id, u16 offset, u16 old_len, old_bytes, u16 new_len, new_bytes` |
| `CLR=5` | 同 UPDATE，但语义为"补偿记录"，undo 时跳过其覆盖范围 |
| `CHECKPOINT=6` | `u64 active_xids_len, xids[...]` |

WAL 只追加；checkpoint 成功后允许从头截断（保留 checkpoint 记录）。写入规则：**任何脏页落盘前，必须先把 WAL flush 到该页的 pageLSN**。

## 6. 元数据页（page 0）

```
+0  u32 magic   0x4C4D4E44 ("LMND")
+4  u16 version 1
+6  u16 page_size 4096
+8  u32 next_page_id   下一个可分配页号
+12 u32 catalog_root   目录 JSON 所在的页号（0 表示内联）
+16 u64 next_xid
+24 u64 checkpoint_lsn
```

目录本身序列化为 JSON 存在独立的页链中（小数据量，便于人读与迁移）；页链满则追加新页。
