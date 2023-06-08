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

pub fn main() !void {
    const stderr = std.io.getStdErr().writer();

    if (builtin.mode != .ReleaseSafe) {
        try stderr.print("CLI must be built as ReleaseSafe.\n", .{});
    }

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

    var cmd = .none;
    var pre_cmd_flags = true;
    if (args.next(allocator)) |arg_or_err| {
        const arg = try arg_or_err;

        // Parse pre-command arguments.
        if (std.mem.eql(u8, command[0..2], "--")) {
            _ = (try parse_arg_addresses(allocator, &args, arg, "--addresses", &addresses)) or
                panic("Unrecognized argument: \"{}\"", .{std.zig.fmtEscapes(arg)});
            continue;
        } else {
            pre_cmd_flags = false;
        }

        // Parse command
        if (!pre_cmd_flags) {
            if (std.mem.eql(u8, command, "create-account")) {
                cmd = .create_account;
            } else if (std.mem.eql(u8, command, "get-account")) {
                cmd = .get_account;
            } else if (std.mem.eql(u8, command, "create-transfer")) {
                cmd = .create_transfer;
            } else if (std.mem.eql(u8, command, "get-transfer")) {
                cmd = .get_transfer;
            }
        }

        break;
    }

    switch (cmd) {
        .create_account => create_account(allocator, &addresses, &args),
        _ => try stderr.print("Command must be create-account, get-account, create-transfer, or get-transfer.\n", .{}),
    }

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
    defer client.deinit(allocator);
    var context = try allocator.create(Context);
    context.done = false;

    while (!context.done) {
        client.tick();
        try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
    }
}

fn create_account() void {
    b.batch_accounts.appendAssumeCapacity(.{
        .id = @bitReverse(u128, b.account_index + 1),
        .user_data = 0,
        .reserved = [_]u8{0} ** 48,
        .ledger = 2,
        .code = 1,
        .flags = .{},
        .debits_pending = 0,
        .debits_posted = 0,
        .credits_pending = 0,
        .credits_posted = 0,
    });

    // Submit batch.
    b.send(
        create_accounts,
        .create_accounts,
        std.mem.sliceAsBytes(b.batch_accounts.items),
    );
}

fn create_transfers(b: *Benchmark) void {
    if (b.transfer_index >= b.transfer_count) {
        b.finish();
        return;
    }

    if (b.transfer_index == 0) {
        // Init timer.
        b.timer.reset();
        b.transfer_next_arrival_ns = b.timer.read();
    }

    const random = b.rng.random();

    b.batch_transfers.resize(0) catch unreachable;
    b.transfer_start_ns.resize(0) catch unreachable;

    // Busy-wait for at least one transfer to be available.
    while (b.transfer_next_arrival_ns >= b.timer.read()) {}
    b.batch_start_ns = b.timer.read();

    // Fill batch.
    while (b.transfer_index < b.transfer_count and
        b.batch_transfers.items.len < transfer_count_per_batch and
        b.transfer_next_arrival_ns < b.batch_start_ns)
    {
        const debit_account_index = random.uintLessThan(u64, b.account_count);
        var credit_account_index = random.uintLessThan(u64, b.account_count);
        if (debit_account_index == credit_account_index) {
            credit_account_index = (credit_account_index + 1) % b.account_count;
        }
        assert(debit_account_index != credit_account_index);
        b.batch_transfers.appendAssumeCapacity(.{
            // Reverse the bits to stress non-append-only index for `id`.
            .id = @bitReverse(u128, b.transfer_index + 1),
            .debit_account_id = @bitReverse(u128, debit_account_index + 1),
            .credit_account_id = @bitReverse(u128, credit_account_index + 1),
            .user_data = random.int(u128),
            .reserved = 0,
            // TODO Benchmark posting/voiding pending transfers.
            .pending_id = 0,
            .timeout = 0,
            .ledger = 2,
            .code = random.int(u16) +| 1,
            .flags = .{},
            .amount = random_int_exponential(random, u64, 10_000) +| 1,
            .timestamp = 0,
        });
        b.transfer_start_ns.appendAssumeCapacity(b.transfer_next_arrival_ns);

        b.transfer_index += 1;
        b.transfer_next_arrival_ns += random_int_exponential(random, u64, b.transfer_arrival_rate_ns);
    }

    assert(b.batch_transfers.items.len > 0);

    // Submit batch.
    b.send(
        create_transfers_finish,
        .create_transfers,
        std.mem.sliceAsBytes(b.batch_transfers.items),
    );
}

fn create_transfers_finish(b: *Benchmark) void {
    // Record latencies.
    const batch_end_ns = b.timer.read();
    const ms_time = @divTrunc(batch_end_ns - b.batch_start_ns, std.time.ns_per_ms);

    if (b.print_batch_timings) {
        log.info("batch {}: {} tx in {} ms\n", .{
            b.batch_index,
            b.batch_transfers.items.len,
            ms_time,
        });
    }

    b.batch_latency_ns.appendAssumeCapacity(batch_end_ns - b.batch_start_ns);
    for (b.transfer_start_ns.items) |start_ns| {
        b.transfer_latency_ns.appendAssumeCapacity(batch_end_ns - start_ns);
    }

    b.batch_index += 1;
    b.transfers_sent += b.batch_transfers.items.len;

    if (b.statsd) |statsd| {
        statsd.gauge("benchmark.txns", b.batch_transfers.items.len) catch {};
        statsd.timing("benchmark.timings", ms_time) catch {};
        statsd.gauge("benchmark.batch", b.batch_index) catch {};
        statsd.gauge("benchmark.completed", b.transfers_sent) catch {};
    }

    b.create_transfers();
}

fn send(
    b: *Benchmark,
    callback: fn (*Benchmark) void,
    operation: StateMachine.Operation,
    payload: []u8,
) void {
    b.callback = callback;
    b.message = b.client.get_message();

    stdx.copy_disjoint(
        .inexact,
        u8,
        b.message.?.buffer[@sizeOf(vsr.Header)..],
        payload,
    );

    b.client.request(
        @intCast(u128, @ptrToInt(b)),
        send_complete,
        operation,
        b.message.?,
        payload.len,
    );
}

fn send_complete(
    user_data: u128,
    operation: StateMachine.Operation,
    result: Client.Error![]const u8,
) void {
    _ = operation;

    const result_payload = result catch |err|
        panic("Client returned error: {}", .{err});

    switch (operation) {
        .create_accounts => {
            const create_accounts_results = std.mem.bytesAsSlice(
                tb.CreateAccountsResult,
                result_payload,
            );
            if (create_accounts_results.len > 0) {
                panic("CreateAccountsResults: {any}", .{create_accounts_results});
            }
        },
        .create_transfers => {
            const create_transfers_results = std.mem.bytesAsSlice(
                tb.CreateTransfersResult,
                result_payload,
            );
            if (create_transfers_results.len > 0) {
                panic("CreateTransfersResults: {any}", .{create_transfers_results});
            }
        },
        else => unreachable,
    }

    const b = @intToPtr(*Benchmark, @intCast(u64, user_data));

    b.client.unref(b.message.?);
    b.message = null;

    const callback = b.callback.?;
    b.callback = null;
    callback(b);
}
