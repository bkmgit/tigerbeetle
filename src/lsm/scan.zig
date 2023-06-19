const std = @import("std");
const mem = std.mem;
const meta = std.meta;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.scan);
const tracer = @import("../tracer.zig");

const constants = @import("../constants.zig");

const stdx = @import("../stdx.zig");
const binary_search = @import("binary_search.zig");
const BinarySearchRange = binary_search.BinarySearchRange;
const lsm = @import("tree.zig");
const Direction = @import("direction.zig").Direction;
const GridType = @import("grid.zig").GridType;
const TableInfoType = @import("manifest.zig").TableInfoType;
const ManifestType = @import("manifest.zig").ManifestType;
const LevelIteratorType = @import("level_iterator.zig").LevelIteratorType;
const KWayMergeIteratorType = @import("k_way_merge.zig").KWayMergeIteratorType;
const ScanContextType = @import("scan_context.zig").ScanContextType;

/// Scans a range of keys over a tree, in ascending or descending order.
pub fn ScanType(
    comptime Table: type,
    comptime Tree: type,
    comptime Storage: type,
) type {
    return struct {
        const Scan = @This();

        const Grid = GridType(Storage);
        const BlockPtr = Grid.BlockPtr;
        const BlockPtrConst = Grid.BlockPtrConst;
        const ScanContext = ScanContextType(Storage);
        const ScanBuffer = ScanContext.ScanBuffer;

        const TableInfo = TableInfoType(Table);
        const Manifest = ManifestType(Table, Storage);
        const LevelIterator = LevelIteratorType(Table, Storage);

        const Key = Table.Key;
        const Value = Table.Value;
        const compare_keys = Table.compare_keys;
        const key_from_value = Table.key_from_value;
        const tombstone = Table.tombstone;

        pub const Callback = fn (*ScanContext, ?Value) void;

        const KWayMergeIterator = KWayMergeIteratorType(
            Scan,
            Key,
            Value,
            key_from_value,
            compare_keys,
            KWayMergeStreams.streams_count,
            merge_stream_peek,
            merge_stream_pop,
            merge_stream_precedence,
        );

        const KWayMergeStreams = enum(u32) {
            table_mutable = constants.lsm_levels,
            table_immutable = constants.lsm_levels + 1,

            /// Lsm levels are represented as a non-exhaustive enum.
            _,

            const streams_count = constants.lsm_levels + 2;

            pub inline fn level(value: KWayMergeStreams) u32 {
                const int = @enumToInt(value);
                assert(int < constants.lsm_levels);

                return int;
            }

            /// Mutable and immutable tables have precedence over lsm levels.
            pub inline fn precedence(a: u32, b: u32) bool {
                assert(a != b);
                assert(a < streams_count and b < streams_count);

                return switch (@intToEnum(KWayMergeStreams, a)) {
                    .table_mutable => true,
                    .table_immutable => @intToEnum(KWayMergeStreams, b) != .table_mutable,
                    else => a < b and b < constants.lsm_levels,
                };
            }
        };

        const Cursor = struct {
            range: BinarySearchRange,
            index: ?u32,

            pub inline fn init(range: BinarySearchRange, direction: Direction) Cursor {
                return .{
                    .range = range,
                    .index = if (range.count == 0) null else switch (direction) {
                        .ascending => 0,
                        .descending => range.count - 1,
                    },
                };
            }

            pub inline fn slice(self: *const Cursor, items: anytype) blk: {
                assert(meta.trait.isIndexable(@TypeOf(items)));
                const T = meta.Child(@TypeOf(items));
                break :blk []const T;
            } {
                return items[self.range.start..][0..self.range.count];
            }

            pub inline fn empty(self: *const Cursor) bool {
                return self.range.count == 0;
            }

            pub inline fn get(self: *const Cursor, items: anytype) blk: {
                assert(meta.trait.isIndexable(@TypeOf(items)));
                const T = meta.Child(@TypeOf(items));
                break :blk ?*const T;
            } {
                return if (self.index) |index|
                    &self.slice(items)[index]
                else
                    null;
            }

            pub inline fn move(self: *Cursor, direction: Direction) bool {
                assert(self.index != null);
                assert(!self.empty());
                assert(self.index.? < self.range.count);

                switch (direction) {
                    .ascending => {
                        const next_index = self.index.? + 1;
                        if (next_index == self.range.count) {
                            self.index = null;
                            return false;
                        } else {
                            self.index = next_index;
                            return true;
                        }
                    },
                    .descending => {
                        if (self.index.? == 0) {
                            self.index = null;
                            return false;
                        } else {
                            self.index.? -= 1;
                            return true;
                        }
                    },
                }
            }
        };

        const LevelScan = struct {
            scan: *Scan,
            iterator: LevelIterator,
            buffer: ScanContext.LevelBuffer,
            cursor: union(enum) {
                /// Loading index_block from storage.
                load,

                /// Moving LevelIterator to the next position.
                next: Cursor,

                /// Positioned at a valid index.
                current: Cursor,

                /// There is no data or it has reached the end.
                eof,
            },
        };

        const LevelState = union(enum) {
            idle: void,
            seeking: *ScanContext,
            fetching: struct {
                context: *ScanContext,
                callback: Callback,
                pending_count: u32,
            },
        };

        grid: *Grid,
        tree: *Tree,
        next_tick: Grid.NextTick = undefined,

        direction: Direction,

        // TODO It's a temporary solution until
        // we can iterate over table mutable in sorted order.
        table_mutable_values: []const Table.Value,
        table_mutable_cursor: Cursor,

        table_immutable_cursor: Cursor,

        level_state: LevelState,
        level_scans: [constants.lsm_levels]LevelScan,

        merge_iterator: ?KWayMergeIterator,
        tracer_slot: ?tracer.SpanStart,

        pub fn reset(scan: *Scan) void {
            assert(scan.level_state == .seeking);
            scan.level_state = .idle;
            scan.merge_iterator = null;
        }

        pub fn init() Scan {
            return .{
                .grid = undefined,
                .tree = undefined,
                .direction = undefined,
                .table_mutable_values = undefined,
                .table_mutable_cursor = undefined,
                .table_immutable_cursor = undefined,
                .level_state = .idle,
                .level_scans = undefined,
                .merge_iterator = null,
                .tracer_slot = undefined,
            };
        }

        pub fn seek(
            scan: *Scan,
            context: *ScanContext,
            grid: *Grid,
            tree: *Tree,
            snapshot: u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) void {
            assert(scan.level_state == .idle);
            assert(scan.merge_iterator == null);

            // TODO It's a temporary solution until
            // we can iterate over table mutable in sorted order.
            const table_mutable_values = tree.table_mutable.sort_into_values();

            scan.* = .{
                .grid = grid,
                .tree = tree,
                .direction = direction,

                .table_mutable_values = table_mutable_values,
                .table_mutable_cursor = Cursor.init(binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    compare_keys,
                    table_mutable_values,
                    key_min,
                    key_max,
                    .{},
                ), direction),

                .table_immutable_cursor = Cursor.init(binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    compare_keys,
                    tree.table_immutable.values,
                    key_min,
                    key_max,
                    .{},
                ), direction),

                .level_state = .{ .seeking = context },
                .level_scans = level_scans: {
                    const buffer = context.get_buffer();
                    for (scan.level_scans) |*level, i| {
                        level.* = .{
                            .scan = scan,
                            .iterator = LevelIterator.init(),
                            .buffer = buffer.levels[i],
                            .cursor = .load,
                        };

                        level.iterator.start(
                            .{
                                .grid = grid,
                                .manifest = &tree.manifest,
                                .level = @intCast(u8, i),
                                .snapshot = snapshot,
                                .key_min = key_min,
                                .key_max = key_max,
                                .direction = direction,
                            },
                            level.buffer.index_block,
                        );
                    }

                    // Don't move level_scans during initialization.
                    break :level_scans scan.level_scans;
                },

                .merge_iterator = null,
                .tracer_slot = scan.tracer_slot,
            };
        }

        pub fn fetch(scan: *Scan, callback: Callback) void {
            assert(scan.level_state == .seeking);
            const context = scan.level_state.seeking;

            scan.level_state = .{
                .fetching = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            // Track an extra "level" that will finish after the loop.
            // This allows us to call the callback if there's no more levels to fetch.
            scan.level_state.fetching.pending_count += 1;

            for (scan.level_scans) |*level| {
                switch (level.cursor) {
                    .load, .next => {
                        scan.level_state.fetching.pending_count += 1;
                        level.iterator.next(.{
                            .on_index = on_level_index_block,
                            .on_data = on_level_data_block,
                        });
                    },
                    else => {},
                }
            }

            scan.level_state.fetching.pending_count -= 1;
            if (scan.level_state.fetching.pending_count == 0) {
                scan.grid.on_next_tick(
                    on_next_tick,
                    &scan.next_tick,
                );
            }
        }

        fn on_next_tick(next_tick: *Grid.NextTick) void {
            const scan = @fieldParentPtr(Scan, "next_tick", next_tick);
            scan.on_fetch();
        }

        fn on_fetch(scan: *Scan) void {
            assert(scan.level_state == .fetching);
            assert(scan.level_state.fetching.pending_count == 0);

            const context = scan.level_state.fetching.context;
            const callback = scan.level_state.fetching.callback;
            scan.level_state = .{ .seeking = context };

            if (scan.merge_iterator == null) {
                scan.merge_iterator = KWayMergeIterator.init(
                    scan,
                    KWayMergeStreams.streams_count,
                    scan.direction,
                );
            }

            const value = scan.merge_iterator.?.pop();
            callback(context, value);
        }

        pub fn on_level_index_block(
            iterator: *LevelIterator,
            table_info: TableInfo,
            index_block: BlockPtrConst,
        ) LevelIterator.DataBlockAddresses {
            _ = table_info;
            const level = @fieldParentPtr(LevelScan, "iterator", iterator);
            assert(level.cursor == .load);
            assert(level.scan.level_state == .fetching);
            assert(level.scan.level_state.fetching.pending_count > 0);

            const cursor = Cursor.init(binary_search.binary_search_keys_range(
                Key,
                compare_keys,
                Table.index_data_keys_used(index_block),
                iterator.context.key_min,
                iterator.context.key_max,
                .{},
            ), level.scan.direction);

            level.cursor = if (cursor.empty()) .eof else .{ .next = cursor };
            return .{
                .addresses = cursor.slice(
                    Table.index_data_addresses_used(index_block),
                ),
                .checksums = cursor.slice(
                    Table.index_data_checksums_used(index_block),
                ),
            };
        }

        pub fn on_level_data_block(iterator: *LevelIterator, data_block: ?BlockPtrConst) void {
            const level = @fieldParentPtr(LevelScan, "iterator", iterator);
            assert(level.scan.level_state == .fetching);
            assert(level.scan.level_state.fetching.pending_count > 0);
            defer {
                level.scan.level_state.fetching.pending_count -= 1;
                if (level.scan.level_state.fetching.pending_count == 0) level.scan.on_fetch();
            }

            if (data_block) |data| {
                stdx.copy_disjoint(.exact, u8, level.buffer.data_block, data);
                switch (level.cursor) {
                    .next => |cursor| level.cursor = .{ .current = cursor },
                    else => unreachable,
                }
            } else {
                level.cursor = .eof;
            }
        }

        fn merge_stream_peek(
            scan: *const Scan,
            stream_index: u32,
        ) error{ Empty, Drained }!Key {
            assert(scan.level_state == .seeking);
            assert(stream_index < KWayMergeStreams.streams_count);

            return switch (@intToEnum(KWayMergeStreams, stream_index)) {
                .table_mutable => scan.merge_table_mutable_peek(),
                .table_immutable => scan.merge_table_immutable_peek(),
                _ => |stream| scan.merge_level_peek(stream.level()),
            };
        }

        fn merge_table_mutable_peek(scan: *const Scan) error{ Empty, Drained }!Key {
            if (scan.table_mutable_cursor.range.count == 0) return error.Empty;

            const value: *const Value = scan.table_mutable_cursor.get(
                scan.table_mutable_values,
            ) orelse return error.Drained;
            return key_from_value(value);
        }

        fn merge_table_immutable_peek(scan: *const Scan) error{ Empty, Drained }!Key {
            if (scan.table_immutable_cursor.range.count == 0) return error.Empty;

            const value: *const Value = scan.table_immutable_cursor.get(
                scan.tree.table_immutable.values,
            ) orelse return error.Drained;
            return key_from_value(value);
        }

        fn merge_level_peek(scan: *const Scan, level_index: u32) error{ Empty, Drained }!Key {
            var level = &scan.level_scans[level_index];
            switch (level.cursor) {
                .load, .next => unreachable,
                .current => |cursor| {
                    const key = cursor.get(
                        Table.index_data_keys_used(level.buffer.index_block),
                    ) orelse return error.Drained;
                    return key.*;
                },
                .eof => return error.Empty,
            }
        }

        fn merge_stream_pop(scan: *Scan, stream_index: u32) Value {
            assert(scan.level_state == .seeking);

            return switch (@intToEnum(KWayMergeStreams, stream_index)) {
                .table_mutable => scan.merge_table_mutable_pop(),
                .table_immutable => scan.merge_table_immutable_pop(),
                _ => |stream| scan.merge_level_pop(stream.level()),
            };
        }

        fn merge_table_mutable_pop(scan: *Scan) Value {
            const value = scan.table_mutable_cursor.get(
                scan.table_mutable_values,
            ) orelse unreachable;

            _ = scan.table_mutable_cursor.move(scan.direction);
            return value.*;
        }

        fn merge_table_immutable_pop(scan: *Scan) Value {
            const value = scan.table_immutable_cursor.get(
                scan.tree.table_immutable.values,
            ) orelse unreachable;

            _ = scan.table_immutable_cursor.move(scan.direction);
            return value.*;
        }

        fn merge_level_pop(scan: *Scan, level_index: u32) Value {
            var level = &scan.level_scans[level_index];
            switch (level.cursor) {
                .current => |*cursor| {
                    const key = cursor.get(
                        Table.index_data_keys_used(level.buffer.index_block),
                    ) orelse unreachable;

                    const value = Table.data_block_search(level.buffer.data_block, key.*);
                    assert(value != null);

                    level.cursor = if (cursor.move(scan.direction)) .{
                        .next = cursor.*,
                    } else .load;
                    return value.?.*;
                },
                else => unreachable,
            }
        }

        fn merge_stream_precedence(scan: *const Scan, a: u32, b: u32) bool {
            _ = scan;
            return KWayMergeStreams.precedence(a, b);
        }
    };
}
