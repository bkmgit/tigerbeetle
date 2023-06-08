const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const panic = std.debug.panic;
const log = std.log;
pub const log_level: std.log.Level = .info;

const constants = @import("constants.zig");
const stdx = @import("stdx.zig");
const IO = @import("io.zig").IO;
const Storage = @import("storage.zig").Storage;
const MessagePool = @import("message_pool.zig").MessagePool;
const MessageBus = @import("message_bus.zig").MessageBusClient;
const StateMachine = @import("state_machine.zig").StateMachineType(Storage, constants.state_machine_config);
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const vsr = @import("vsr.zig");
const Client = vsr.Client(StateMachine, MessageBus);
const tb = @import("tigerbeetle.zig");

const account_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Account),
);
const transfer_count_per_batch = @divExact(
    constants.message_size_max - @sizeOf(vsr.Header),
    @sizeOf(tb.Transfer),
);

fn parse_arg_addresses(
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
    arg: []const u8,
    arg_name: []const u8,
    arg_value: *[]std.net.Address,
) !bool {
    if (!std.mem.eql(u8, arg, arg_name)) return false;

    allocator.free(arg_value.*);

    const address_string_or_err = args.next(allocator) orelse
        panic("Expected an argument to {s}", .{arg_name});
    const address_string = try address_string_or_err;
    arg_value.* = try vsr.parse_addresses(allocator, address_string, constants.nodes_max);
    return true;
}

fn parse_arg_usize(
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
    arg: []const u8,
    arg_name: []const u8,
    arg_value: *usize,
) !bool {
    if (!std.mem.eql(u8, arg, arg_name)) return false;

    const int_string_or_err = args.next(allocator) orelse
        panic("Expected an argument to {s}", .{arg_name});
    const int_string = try int_string_or_err;
    arg_value.* = std.fmt.parseInt(usize, int_string, 10) catch |err|
        panic(
        "Could not parse \"{}\" as an integer: {}",
        .{ std.zig.fmtEscapes(int_string), err },
    );
    return true;
}

fn parse_arg_bool(
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
    arg: []const u8,
    arg_name: []const u8,
    arg_value: *bool,
) !bool {
    if (!std.mem.eql(u8, arg, arg_name)) return false;

    const bool_string_or_err = args.next(allocator) orelse
        panic("Expected an argument to {s}", .{arg_name});
    const bool_string = try bool_string_or_err;
    arg_value.* = std.mem.eql(u8, bool_string, "true");

    return true;
}

const Context = struct {
    done: bool,
    client: *Client,
    message: ?*MessagePool.Message,
};

const Command = enum {
    none,
    create_account,
    lookup_account,
    create_transfer,
    lookup_transfer,
};

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const allocator = arena.allocator();

    var addresses = try allocator.alloc(std.net.Address, 1);
    addresses[0] = try std.net.Address.parseIp4("127.0.0.1", constants.port);

    // This will either free the above address alloc, or parse_arg_addresses will
    // free and re-alloc internally and this will free that.
    defer allocator.free(addresses);

    var args = std.process.args();

    // Discard executable name.
    _ = try args.next(allocator).?;

    var cmd: Command = .none;
    var pre_cmd_flags = true;
    while (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;

        // Parse pre-command arguments.
        if (std.mem.eql(u8, arg[0..2], "--")) {
            _ = (try parse_arg_addresses(allocator, &args, arg, "--addresses", &addresses)) or
                panic("Unrecognized argument: \"{}\"", .{std.zig.fmtEscapes(arg)});
            continue;
        } else {
            pre_cmd_flags = false;
        }

        // Parse command
        if (!pre_cmd_flags) {
            if (std.mem.eql(u8, arg, "create-account")) {
                cmd = .create_account;
            } else if (std.mem.eql(u8, arg, "lookup-account")) {
                cmd = .lookup_account;
            } else if (std.mem.eql(u8, arg, "create-transfer")) {
                cmd = .create_transfer;
            } else if (std.mem.eql(u8, arg, "lookup-transfer")) {
                cmd = .lookup_transfer;
            }
        }

        break;
    }

    var context = try allocator.create(Context);
    context.done = false;

    const client_id = std.crypto.random.int(u128);
    const cluster_id: u32 = 0;

    var io = try IO.init(32, 0);

    var message_pool = try MessagePool.init(allocator, .client);

    var client = try Client.init(
        allocator,
        client_id,
        cluster_id,
        @intCast(u8, addresses.len),
        &message_pool,
        .{
            .configuration = addresses,
            .io = &io,
        },
    );
    context.client = &client;

    switch (cmd) {
        .create_account => try create_account(allocator, &args, context),
        else => try panic("Command must be create-account, get-account, create-transfer, or get-transfer.\n", .{}),
    }

    while (!context.done) {
        context.client.tick();
        try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

fn create_account(
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
    context: *Context,
) !void {
    var batch_accounts = try std.ArrayList(tb.Account).initCapacity(allocator, 1);
    var account = tb.Account{
        .id = 0,
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .ledger = 0,
        .code = 0,
        .flags = .{},
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    };
    if (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;

        // Parse account fields
        if (std.mem.eql(u8, arg[0..2], "id:")) {
            account.id = try std.fmt.parseInt(u128, arg[3..], 10);
        }

        if (std.mem.eql(u8, arg[0..9], "user_data:")) {
            account.user_data = try std.fmt.parseInt(u128, arg[10..], 10);
        }

        if (std.mem.eql(u8, arg[0..6], "ledger:")) {
            account.ledger = try std.fmt.parseInt(u32, arg[7..], 10);
        }

        if (std.mem.eql(u8, arg[0..4], "code:")) {
            account.code = try std.fmt.parseInt(u16, arg[5..], 10);
        }

        if (std.mem.eql(u8, arg[0..5], "flags:")) {
            // TODO: is @bitCast right?
            account.flags = @bitCast(tb.AccountFlags, try std.fmt.parseInt(u16, arg[6..], 10));
        }

        if (std.mem.eql(u8, arg[0..14], "debits_pending:")) {
            account.debits_pending = try std.fmt.parseInt(u64, arg[15..], 10);
        }

        if (std.mem.eql(u8, arg[0..13], "debits_posted:")) {
            account.debits_posted = try std.fmt.parseInt(u64, arg[14..], 10);
        }

        if (std.mem.eql(u8, arg[0..15], "credits_pending:")) {
            account.credits_pending = try std.fmt.parseInt(u64, arg[16..], 10);
        }

        if (std.mem.eql(u8, arg[0..14], "credits_posted:")) {
            account.credits_posted = try std.fmt.parseInt(u64, arg[15..], 10);
        }
    }

    // Submit batch.
    send(
        context,
        .create_accounts,
        std.mem.sliceAsBytes(batch_accounts.items),
    );
}

fn send(
    context: *Context,
    operation: StateMachine.Operation,
    payload: []u8,
) void {
    context.message = context.client.get_message();

    stdx.copy_disjoint(
        .inexact,
        u8,
        context.message.?.buffer[@sizeOf(vsr.Header)..],
        payload,
    );

    context.client.request(
        @intCast(u128, @ptrToInt(context)),
        send_complete,
        operation,
        context.message.?,
        payload.len,
    );
}

fn send_complete(
    user_data: u128,
    operation: StateMachine.Operation,
    result: Client.Error![]const u8,
) void {
    const result_payload = result catch |err|
        panic("Client returned error: {}", .{err});

    switch (operation) {
        .create_accounts => {
            const create_account_results = std.mem.bytesAsSlice(
                tb.CreateAccountsResult,
                result_payload,
            );
            if (create_account_results.len > 0) {
                panic("CreateAccountsesults: {any}", .{create_account_results});
            }

            std.debug.print("Ok!\n", .{});
        },
        .lookup_accounts => {
            const lookup_account_results = std.mem.bytesAsSlice(
                tb.Account,
                result_payload,
            );
            if (lookup_account_results.len != 0) {
                panic("Failed to lookup account.", .{});
            }

            std.debug.print("{any}", .{lookup_account_results});
        },
        else => unreachable,
    }

    const context = @intToPtr(*Context, @intCast(u64, user_data));

    context.client.unref(context.message.?);
    context.message = null;

    context.done = true;
}
