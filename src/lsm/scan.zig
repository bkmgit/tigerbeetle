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
                    _ => a < b and b < @enumToInt(KWayMergeStreams.table_mutable),
                };
            }
        };

        const Cursor = struct {
            range: BinarySearchRange,
            index: u32 = 0,

            pub inline fn slice(self: *const Cursor, items: anytype) blk: {
                assert(meta.trait.isIndexable(@TypeOf(items)));
                const T = meta.Child(@TypeOf(items));
                break :blk []const T;
            } {
                return items[self.range.index_begin..self.range.index_end];
            }

            pub inline fn item(self: *const Cursor, items: anytype) blk: {
                assert(meta.trait.isIndexable(@TypeOf(items)));
                const T = meta.Child(@TypeOf(items));
                break :blk ?*const T;
            } {
                return if (self.index < self.len())
                    &self.slice(items)[self.index]
                else
                    null;
            }

            pub inline fn len(self: *const Cursor) u32 {
                return self.range.index_end - self.range.index_begin;
            }

            pub inline fn move_next(self: *Cursor) bool {
                assert(self.index < self.len());

                self.index += 1;
                return self.index < self.len();
            }
        };

        const ScanLevel = struct {
            scan: *Scan,
            iterator: LevelIterator,
            buffer: ScanContext.LevelBuffer,
            cursor: union(enum) {
                /// Loading index_block from disk.
                load,

                /// Moving LevelIterator to the next position.
                next: Cursor,

                /// Positioned at a valid index.
                current: Cursor,

                /// There is no data or it has reached the end.
                eof,
            },
        };

        const ScanLevelState = union(enum) {
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

        table_immutable: ?Cursor,
        disk: struct {
            state: ScanLevelState,
            levels: [constants.lsm_levels]ScanLevel,
        },

        merge_iterator: ?KWayMergeIterator,
        tracer_slot: ?tracer.SpanStart,

        pub fn reset(scan: *Scan) void {
            assert(scan.disk.state == .seeking);
            scan.disk.state = .idle;
            scan.merge_iterator = null;
        }

        pub fn init() Scan {
            return .{
                .grid = undefined,
                .tree = undefined,
                .table_immutable = null,
                .disk = .{
                    .state = .idle,
                    .levels = undefined,
                },
                .merge_iterator = null,
                .tracer_slot = undefined,
            };
        }

        pub fn seek(
            scan: *Scan,
            context: *ScanContext,
            grid: *Grid,
            tree: *Tree,
            snapshot: ?u64,
            key_min: Key,
            key_max: Key,
            direction: Direction,
        ) void {
            assert(scan.disk.state == .idle);
            assert(scan.merge_iterator == null);

            scan.* = .{
                .grid = grid,
                .tree = tree,
                .table_immutable = if (binary_search.binary_search_values_range(
                    Key,
                    Value,
                    key_from_value,
                    compare_keys,
                    scan.tree.table_immutable.values,
                    key_min,
                    key_max,
                    .{},
                )) |range| .{
                    .range = range,
                    .index = 0,
                } else null,
                .disk = .{
                    .state = .{ .seeking = context },
                    .levels = scan.disk.levels,
                },
                .merge_iterator = null,
                .tracer_slot = scan.tracer_slot,
            };

            const buffer = context.get_buffer();
            for (scan.disk.levels) |*level, i| {
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
                        .snapshot = snapshot orelse lsm.snapshot_latest,
                        .key_min = key_min,
                        .key_max = key_max,
                        .direction = direction,
                    },
                    level.buffer.index_block,
                );
            }
        }

        pub fn fetch(scan: *Scan, callback: Callback) void {
            assert(scan.disk.state == .seeking);
            const context = scan.disk.state.seeking;

            scan.disk.state = .{
                .fetching = .{
                    .context = context,
                    .callback = callback,
                    .pending_count = 0,
                },
            };

            // Track an extra "level" that will finish after the loop.
            // This allows us to call the callback if there's no more levels to fetch.
            scan.disk.state.fetching.pending_count += 1;

            for (scan.disk.levels) |*level| {
                switch (level.cursor) {
                    .load, .next => {
                        scan.disk.state.fetching.pending_count += 1;
                        level.iterator.next(.{
                            .on_index = on_level_index_block,
                            .on_data = on_level_data_block,
                        });
                    },
                    else => {},
                }
            }

            scan.disk.state.fetching.pending_count -= 1;
            if (scan.disk.state.fetching.pending_count == 0) {
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
            assert(scan.disk.state == .fetching);
            assert(scan.disk.state.fetching.pending_count == 0);

            const context = scan.disk.state.fetching.context;
            const callback = scan.disk.state.fetching.callback;
            scan.disk.state = .{ .seeking = context };

            if (scan.merge_iterator == null) {
                scan.merge_iterator = KWayMergeIterator.init(
                    scan,
                    KWayMergeStreams.streams_count,
                    .ascending,
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
            const level = @fieldParentPtr(ScanLevel, "iterator", iterator);
            assert(level.cursor == .load);
            assert(level.scan.disk.state == .fetching);
            assert(level.scan.disk.state.fetching.pending_count > 0);

            const keys = Table.index_data_keys_used(index_block);
            if (binary_search.binary_search_keys_range(
                Key,
                compare_keys,
                keys,
                iterator.context.key_min,
                iterator.context.key_max,
                .{},
            )) |range| {
                level.cursor = .{ .next = Cursor{ .range = range } };
                return .{
                    .addresses = level.cursor.next.slice(
                        Table.index_data_addresses_used(index_block),
                    ),
                    .checksums = level.cursor.next.slice(
                        Table.index_data_checksums_used(index_block),
                    ),
                };
            } else {
                level.cursor = .eof;
                return .{
                    .addresses = &[_]u64{},
                    .checksums = &[_]u128{},
                };
            }
        }

        pub fn on_level_data_block(iterator: *LevelIterator, data_block: ?BlockPtrConst) void {
            const level = @fieldParentPtr(ScanLevel, "iterator", iterator);
            assert(level.scan.disk.state == .fetching);
            assert(level.scan.disk.state.fetching.pending_count > 0);
            defer {
                level.scan.disk.state.fetching.pending_count -= 1;
                if (level.scan.disk.state.fetching.pending_count == 0) level.scan.on_fetch();
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
            assert(scan.disk.state == .seeking);
            assert(stream_index < KWayMergeStreams.streams_count);

            return switch (@intToEnum(KWayMergeStreams, stream_index)) {
                .table_mutable => error.Empty,
                .table_immutable => scan.merge_table_immutable_peek(),
                _ => |stream| scan.merge_disk_peek(stream.level()),
            };
        }

        fn merge_table_immutable_peek(scan: *const Scan) error{ Empty, Drained }!Key {
            if (scan.table_immutable) |cursor| {
                const values = scan.tree.table_immutable.values;
                const value = cursor.item(values) orelse return error.Drained;
                return key_from_value(value);
            }
            return error.Empty;
        }

        fn merge_disk_peek(scan: *const Scan, level_index: u32) error{ Empty, Drained }!Key {
            var level = &scan.disk.levels[level_index];
            switch (level.cursor) {
                .load, .next => unreachable,
                .current => |cursor| {
                    const keys = Table.index_data_keys_used(level.buffer.index_block);
                    const key = cursor.item(keys) orelse return error.Drained;
                    return key.*;
                },
                .eof => return error.Empty,
            }
        }

        fn merge_stream_pop(scan: *Scan, stream_index: u32) Value {
            assert(scan.disk.state == .seeking);

            return switch (@intToEnum(KWayMergeStreams, stream_index)) {
                .table_mutable => unreachable,
                .table_immutable => scan.merge_table_immutable_pop(),
                _ => |stream| scan.merge_disk_pop(stream.level()),
            };
        }

        fn merge_table_immutable_pop(scan: *Scan) Value {
            if (scan.table_immutable) |*cursor| {
                const values = scan.tree.table_immutable.values;
                const value = cursor.item(values);
                assert(value != null);

                _ = cursor.move_next();
                return value.?.*;
            }
            unreachable;
        }

        fn merge_disk_pop(scan: *Scan, level_index: u32) Value {
            var level = &scan.disk.levels[level_index];
            switch (level.cursor) {
                .current => |*cursor| {
                    const keys = Table.index_data_keys_used(level.buffer.index_block);
                    const key = cursor.item(keys);
                    assert(key != null);

                    const value = Table.data_block_search(level.buffer.data_block, key.?.*);
                    assert(value != null);

                    level.cursor = if (cursor.move_next()) .{ .next = cursor.* } else .load;
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
