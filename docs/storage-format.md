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
| page header (24B)                                |
+--------------------------------------------------+  24
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

类型 2 = 内部节点，3 = 叶子节点。

```
内部节点:
+0  u16 page_type
+2  u16 nkeys
+4  u32 next_page (同层链，用于顺序扫描)
+8  u32 prev_page
+12 u32 page_id
+16 u32 lsn
+20 u32 child[0]
+24 key[0]  -> child[1]
      key[1]  -> child[2] ...
   语义: child[i] 下的所有 key 满足 key[i-1] <= k < key[i]（边界开区间由比较器决定）

叶子节点:
+0  u16 page_type
+2  u16 nkeys
+4  u32 next_page     ← 叶子链，范围扫描依赖它
+8  u32 prev_page
+12 u32 page_id
+16 u32 lsn
+20 (key, rid) 对，紧凑排列
```

key 编码与列类型一致（INT 用 zigzag varint，VARCHAR 用 len+bytes）。叶子链保证范围扫描只需顺序读叶页。

分裂策略：满页时按中位键分裂，左页保留 `⌈n/2⌉`，右页得到其余，中位键上升到父节点。删除时若节点键数低于 `⌊order/2⌋`，优先向兄弟借位，否则与兄弟合并并递归处理父节点；根节点只剩一个孩子时降低树高。

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
