const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const constants = @import("../constants.zig");
const alloc_block = @import("grid.zig").alloc_block;
const GridType = @import("grid.zig").GridType;

/// ScanContext holds enough memory to perform up to a max number of
/// scans operations in parallel.
/// This buffer is shared across different trees.
pub fn ScanContextType(comptime Storage: type) type {
    return struct {
        const ScanContext = @This();
        const Grid = GridType(Storage);

        pub const scan_max = 10;

        pub const ScanBuffer = struct {
            levels: [constants.lsm_levels]LevelBuffer,

            pub fn init(allocator: Allocator) !ScanBuffer {
                var self: ScanBuffer = undefined;
                for (self.levels) |*level, i| {
                    errdefer for (self.levels[0..i]) |*level_| level_.deinit(allocator);
                    level.* = try LevelBuffer.init(allocator);
                }
                errdefer for (self.levels) |*level| level.deinit(allocator);

                return self;
            }

            pub fn deinit(self: *ScanBuffer, allocator: Allocator) void {
                for (self.levels) |*level| {
                    level.deinit(allocator);
                }
            }
        };

        pub const LevelBuffer = struct {
            index_block: Grid.BlockPtr,
            data_block: Grid.BlockPtr,

            pub fn init(allocator: Allocator) !LevelBuffer {
                var index_block = try alloc_block(allocator);
                errdefer allocator.free(index_block);

                var data_block = try alloc_block(allocator);
                errdefer allocator.free(data_block);

                return LevelBuffer{
                    .index_block = index_block,
                    .data_block = data_block,
                };
            }

            pub fn deinit(self: *LevelBuffer, allocator: Allocator) void {
                allocator.free(self.index_block);
                allocator.free(self.data_block);
            }
        };

        scan_buffers: [scan_max]ScanBuffer,
        scan_buffer_used: u8,

        pub fn init(allocator: Allocator) !ScanContext {
            var scan_buffers: [scan_max]ScanBuffer = undefined;
            for (scan_buffers) |*scan_buffer, i| {
                errdefer for (scan_buffers[0..i]) |*buffer| buffer.deinit(allocator);
                scan_buffer.* = try ScanBuffer.init(allocator);
            }
            errdefer for (scan_contexts[0..i]) |*context| context.deinit(allocator);

            return ScanContext{
                .scan_buffers = scan_buffers,
                .scan_buffer_used = 0,
            };
        }

        pub fn deinit(self: *ScanContext, allocator: Allocator) void {
            for (self.scan_buffers) |*scan_buffer| {
                scan_buffer.deinit(allocator);
            }
        }

        pub inline fn get_buffer(self: *ScanContext) ScanBuffer {
            assert(self.scan_buffer_used < scan_max);
            defer self.scan_buffer_used += 1;
            return self.scan_buffers[self.scan_buffer_used];
        }

        pub inline fn reset(self: *ScanContext) void {
            self.scan_buffer_used = 0;
        }
    };
}
