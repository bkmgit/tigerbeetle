const std = @import("std");
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const div_ceil = @import("../stdx.zig").div_ceil;
const binary_search = @import("binary_search.zig");
const snapshot_latest = @import("tree.zig").snapshot_latest;

pub fn TableImmutableType(comptime Table: type) type {
    const Key = Table.Key;
    const Value = Table.Value;
    const value_count_max = Table.value_count_max;
    const compare_keys = Table.compare_keys;
    const key_from_value = Table.key_from_value;

    return struct {
        const TableImmutable = @This();

        values: []Value,
        snapshot_min: u64,
        free: bool,

        pub fn init(allocator: mem.Allocator) !TableImmutable {
            const values = try allocator.alloc(Value, value_count_max);
            errdefer allocator.free(values);

            return TableImmutable{
                .snapshot_min = undefined,
                .values = values,
                .free = true,
            };
        }

        pub inline fn values_max(table: *const TableImmutable) []Value {
            assert(table.values.len <= value_count_max);
            return table.values.ptr[0..value_count_max];
        }

        pub inline fn key_min(table: *const TableImmutable) Key {
            assert(!table.free);
            assert(table.values.len > 0);
            return key_from_value(&table.values[0]);
        }

        pub inline fn key_max(table: *const TableImmutable) Key {
            assert(!table.free);
            assert(table.values.len > 0);
            return key_from_value(&table.values[table.values.len - 1]);
        }

        pub fn deinit(table: *TableImmutable, allocator: mem.Allocator) void {
            allocator.free(table.values_max());
        }

        pub fn clear(table: *TableImmutable) void {
            // This hack works around the stage1 compiler's problematic handling of pointers to
            // zero-bit types. In particular, `slice = slice[0..0]` is not equivalent to
            // `slice.len = 0` but in fact sets `slice.ptr = undefined` as well. This happens
            // since the type of `slice[0..0]` is `*[0]Value` which is a pointer to a zero-bit
            // type. Using slice bounds that are not comptime known avoids the issue.
            // See: https://github.com/ziglang/zig/issues/6706
            // TODO(zig) Remove this hack when upgrading to 0.10.0.
            var runtime_zero: usize = 0;

            table.* = .{
                .snapshot_min = undefined,
                .values = table.values[runtime_zero..runtime_zero],
                .free = true,
            };
        }

        pub fn reset_with_sorted_values(
            table: *TableImmutable,
            snapshot_min: u64,
            sorted_values: []const Value,
        ) void {
            assert(table.free);
            assert(snapshot_min > 0);
            assert(snapshot_min < snapshot_latest);

            assert(sorted_values.ptr == table.values.ptr);
            assert(sorted_values.len > 0);
            assert(sorted_values.len <= value_count_max);
            assert(sorted_values.len <= Table.data.block_value_count_max * Table.data_block_count_max);

            if (constants.verify) {
                var i: usize = 1;
                while (i < sorted_values.len) : (i += 1) {
                    assert(i > 0);
                    const left_key = key_from_value(&sorted_values[i - 1]);
                    const right_key = key_from_value(&sorted_values[i]);
                    assert(compare_keys(left_key, right_key) == .lt);
                }
            }

            table.* = .{
                .values = table.values.ptr[0..sorted_values.len],
                .snapshot_min = snapshot_min,
                .free = false,
            };
        }

        // TODO(ifreund) This would be great to unit test.
        pub fn get(table: *const TableImmutable, key: Key) ?*const Value {
            assert(!table.free);

            const result = binary_search.binary_search_values(
                Key,
                Value,
                key_from_value,
                compare_keys,
                table.values,
                key,
                .{},
            );
            if (result.exact) {
                const value = &table.values[result.index];
                if (constants.verify) assert(compare_keys(key, key_from_value(value)) == .eq);
                return value;
            }

            return null;
        }

        // TODO: Use custom Iterator tailored to TableImmutable in the future.
        pub const Iterator = @import("compaction.zig").ArrayIteratorType(Value);

        pub fn iterator(table: *const TableImmutable) Iterator {
            return .{ .values = table.values };
        }
    };
}
