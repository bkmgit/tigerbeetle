const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const ManifestType = @import("manifest.zig").ManifestType;
const alloc_block = @import("grid.zig").alloc_block;
const GridType = @import("grid.zig").GridType;
const Direction = @import("direction.zig").Direction;
const TableDataIteratorType = @import("table_data_iterator.zig").TableDataIteratorType;

/// A LevelIterator iterates the data blocks of every table in a key range in ascending key order.
pub fn LevelIteratorType(comptime Table: type, comptime Storage: type) type {
    return struct {
        const LevelIterator = @This();

        const Key = Table.Key;
        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const Manifest = ManifestType(Table, Storage);
        const TableInfo = Manifest.TableInfo;
        const TableDataIterator = TableDataIteratorType(Storage);

        pub const Context = struct {
            grid: *Grid,
            manifest: *Manifest,
            level: u8,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        };

        // A LevelIndexIterator iterates the index blocks of every table in a key range in ascending key order.
        const LevelIndexIterator = struct {
            pub const IndexBlockCallback = fn (
                it: *LevelIndexIterator,
                table_info: ?TableInfo,
                index_block: ?BlockPtrConst,
            ) void;

            /// Passed by `start`.
            context: *const Context,

            /// The key_max (when .ascending) or key_min (when .descending) of the last table iterated.
            /// Used to get the next table from the manifest.
            key_exclusive: ?Key,

            callback: union(enum) {
                none,
                read: struct {
                    callback: IndexBlockCallback,
                    table_info: TableInfo,
                },
                next_tick: IndexBlockCallback,
            },

            read: Grid.Read = undefined,
            next_tick: Grid.NextTick = undefined,

            pub fn init() LevelIndexIterator {
                return LevelIndexIterator{
                    .context = undefined,
                    .key_exclusive = null,
                    .callback = .none,
                };
            }

            pub fn start(it: *LevelIndexIterator, context: *const Context) void {
                assert(it.callback == .none);
                if (context.direction == .descending) {
                    @panic("TODO Implement descending direction for LevelIndexIterator.");
                }

                it.* = .{
                    .context = context,
                    .key_exclusive = null,
                    .callback = .none,
                };
            }

            /// Calls `callback` with either the next index block or null.
            /// The block is only valid for the duration of the callback.
            pub fn next(it: *LevelIndexIterator, callback: IndexBlockCallback) void {
                assert(it.callback == .none);

                // TODO: Fix range on the caller side,
                // It's always populated with table's key_max,
                // for scans we need a smaller range.
                if (it.key_exclusive) |key_exclusive| {
                    if (Table.compare_keys(key_exclusive, it.context.key_max) == .gt or Table.compare_keys(key_exclusive, it.context.key_min) == .lt) {
                        it.callback = .{ .next_tick = callback };
                        it.context.grid.on_next_tick(on_next_tick, &it.next_tick);
                        return;
                    }
                }

                // NOTE We must ensure that between calls to `next`,
                //      no changes are made to the manifest that are visible to `it.context.snapshot`.
                const next_table_info = it.context.manifest.next_table(
                    it.context.level,
                    it.context.snapshot,
                    it.context.key_min,
                    it.context.key_max,
                    it.key_exclusive,
                    it.context.direction,
                );
                if (next_table_info) |table_info| {
                    it.key_exclusive = switch (it.context.direction) {
                        .ascending => table_info.key_max,
                        .descending => table_info.key_min,
                    };
                    it.callback = .{
                        .read = .{
                            .callback = callback,
                            // Copy table_info so we can hold on to it across `read_block`.
                            .table_info = table_info.*,
                        },
                    };
                    it.context.grid.read_block(
                        on_read,
                        &it.read,
                        table_info.address,
                        table_info.checksum,
                        .index,
                    );
                } else {
                    it.callback = .{ .next_tick = callback };
                    it.context.grid.on_next_tick(on_next_tick, &it.next_tick);
                }
            }

            fn on_read(read: *Grid.Read, block: Grid.BlockPtrConst) void {
                const it = @fieldParentPtr(LevelIndexIterator, "read", read);
                assert(it.callback == .read);

                const callback = it.callback.read.callback;
                const table_info = it.callback.read.table_info;
                it.callback = .none;

                callback(it, table_info, block);
            }

            fn on_next_tick(next_tick: *Grid.NextTick) void {
                const it = @fieldParentPtr(LevelIndexIterator, "next_tick", next_tick);
                assert(it.callback == .next_tick);

                const callback = it.callback.next_tick;
                it.callback = .none;
                callback(it, null, null);
            }
        };

        pub const DataBlockAddresses = struct {
            /// Table data block addresses.
            addresses: []const u64,
            /// Table data block checksums.
            checksums: []const u128,
        };
        pub const IndexCallback = fn (
            it: *LevelIterator,
            table_info: TableInfo,
            index_block: BlockPtrConst,
        ) DataBlockAddresses;
        pub const DataCallback = fn (it: *LevelIterator, data_block: ?BlockPtrConst) void;
        pub const Callback = struct {
            on_index: IndexCallback,
            on_data: DataCallback,
        };

        /// Passed by `start`.
        context: Context,

        /// Internal state.
        level_index_iterator: LevelIndexIterator,
        table_data_iterator: TableDataIterator,

        // Local copy of index block, for use in `table_data_iterator`.
        // Passed by `start`.
        index_block: BlockPtr,

        callback: union(enum) {
            none,
            level_index_next: Callback,
            table_data_next: Callback,
        },

        pub fn init() LevelIterator {
            return .{
                .context = undefined,
                .level_index_iterator = LevelIndexIterator.init(),
                .table_data_iterator = TableDataIterator.init(),
                .index_block = undefined,
                .callback = .none,
            };
        }

        pub fn start(
            it: *LevelIterator,
            context: Context,
            index_block_buffer: BlockPtr,
        ) void {
            assert(it.callback == .none);
            it.* = .{
                .context = context,
                .level_index_iterator = it.level_index_iterator,
                .table_data_iterator = it.table_data_iterator,
                .index_block = index_block_buffer,
                .callback = .none,
            };
            it.level_index_iterator.start(&it.context);
            it.table_data_iterator.start(.{
                .grid = context.grid,
                .addresses = &.{},
                .checksums = &.{},
            });
        }

        /// *May* call `callback.on_index` once with the next index block,
        /// if we've finished the previous index block,
        /// or with null if there are no more index blocks in the range.
        ///
        /// *Will* call `callback.on_data` once with the next data block,
        /// or with null if there are no more data blocks in the range.
        ///
        /// For both callbacks, the block is only valid for the duration of the callback.
        pub fn next(it: *LevelIterator, callback: Callback) void {
            assert(it.callback == .none);

            if (it.table_data_iterator.empty()) {
                // Refill `table_data_iterator` before calling `table_next`.
                it.level_index_next(callback);
            } else {
                it.table_data_next(callback);
            }
        }

        inline fn level_index_next(it: *LevelIterator, callback: Callback) void {
            assert(it.callback == .none);

            it.callback = .{ .level_index_next = callback };
            it.level_index_iterator.next(on_level_index_next);
        }

        fn on_level_index_next(
            level_index_iterator: *LevelIndexIterator,
            table_info: ?TableInfo,
            index_block: ?BlockPtrConst,
        ) void {
            const it = @fieldParentPtr(
                LevelIterator,
                "level_index_iterator",
                level_index_iterator,
            );
            assert(it.table_data_iterator.empty());
            const callback = it.callback.level_index_next;
            it.callback = .none;

            if (index_block) |block| {
                // `index_block` is only valid for this callback, so copy it's contents.
                // TODO(jamii) This copy can be avoided if we bypass the cache.
                stdx.copy_disjoint(.exact, u8, it.index_block, block);
                const data_block_addresses = callback.on_index(it, table_info.?, it.index_block);
                it.table_data_iterator.start(.{
                    .grid = it.context.grid,
                    .addresses = data_block_addresses.addresses,
                    .checksums = data_block_addresses.checksums,
                });
            } else {
                // If there are no more index blocks, we can just leave `table_data_iterator` empty.
            }

            it.table_data_next(callback);
        }

        inline fn table_data_next(it: *LevelIterator, callback: Callback) void {
            assert(it.callback == .none);

            it.callback = .{ .table_data_next = callback };
            it.table_data_iterator.next(on_table_data_next);
        }

        fn on_table_data_next(table_data_iterator: *TableDataIterator, data_block: ?Grid.BlockPtrConst) void {
            const it = @fieldParentPtr(LevelIterator, "table_data_iterator", table_data_iterator);
            const callback = it.callback.table_data_next;
            it.callback = .none;

            callback.on_data(it, data_block);
        }
    };
}
