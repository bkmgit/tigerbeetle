const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");

const vsr = @import("vsr");
const IO = vsr.io.IO;
const MessagePool = vsr.message_pool.MessagePool;

const tb = vsr.tigerbeetle;

pub fn ClientType(comptime StateMachine: type, comptime MessageBus: type) type {
    return struct {
        const Client = vsr.Client(StateMachine, MessageBus);

        fn panic(comptime m: []const u8, args: anytype) noreturn {
            const stderr = std.io.getStdErr().writer();
            stderr.print(m, args) catch unreachable;
            std.os.exit(1);
        }

        const Context = struct {
            done: bool,
            repl: bool,
            client: *Client,
            message: ?*MessagePool.Message,
        };

        const Command = enum {
            none,
            create_accounts,
            lookup_accounts,
            create_transfers,
            lookup_transfers,
        };

        fn match_command(arg: []const u8) Command {
            if (std.mem.eql(u8, arg, "create-accounts")) {
                return .create_accounts;
            } else if (std.mem.eql(u8, arg, "lookup-accounts")) {
                return .lookup_accounts;
            } else if (std.mem.eql(u8, arg, "create-transfers")) {
                return .create_transfers;
            } else if (std.mem.eql(u8, arg, "lookup-transfers")) {
                return .lookup_transfers;
            }

            panic("Command must be create-accounts, lookup-accounts, create-transfers, or lookup-transfers.\n", .{});
        }

        fn do_command(
            arena: *std.heap.ArenaAllocator,
            rest: []const [:0]const u8,
            context: *Context,
            cmd: Command,
        ) !void {
            switch (cmd) {
                .create_accounts => try create_accounts(arena, rest, context),
                .lookup_accounts => try lookup_accounts(arena, rest, context),
                .create_transfers => try create_transfers(arena, rest, context),
                .lookup_transfers => try lookup_transfers(arena, rest, context),
                else => panic("Command not yet implemented.", .{}),
            }
        }

        fn repl(
            arena: *std.heap.ArenaAllocator,
            context: *Context,
        ) !void {
            const stdout = std.io.getStdOut().writer();

            var done = false;
            while (!done) {
                try stdout.print("> ", .{});

                const in = std.io.getStdIn();
                var stream = std.io.bufferedReader(in.reader()).reader();

                var input = std.ArrayList(u8).init(arena.allocator());
                var buf: [4096]u8 = undefined;

                if (stream.readUntilDelimiterOrEof(&buf, ';')) |bytes| {
                    if (bytes) |b| {
                        try input.appendSlice(b);
                    }
                } else |err| {
                    done = true;
                    context.done = true;
                    return err;
                }

                var cmd: Command = .none;
                var args = std.ArrayList([:0]const u8).init(arena.allocator());
                var current_arg = std.ArrayList(u8).init(arena.allocator());
                var in_arg = false;

                for (input.items) |c, i| {
                    if (c == ' ' and cmd == .none) {
                        cmd = match_command(input.items[0..i]);
                    }

                    if (cmd != .none) {
                        if (c == '"') {
                            // Done one arg, start another
                            if (in_arg) {
                                try args.append(current_arg.items[0.. :0]);
                                // Reset the current arg.
                                current_arg.clearRetainingCapacity();
                                in_arg = false;

                                // Skip current "
                                continue;
                            } else {
                                in_arg = true;

                                // Skip current "
                                continue;
                            }
                        }
                    }

                    if (in_arg) {
                        try current_arg.append(c);
                    }
                }

                try do_command(
                    arena,
                    args.items,
                    context,
                    cmd,
                );
            }
        }

        pub fn run(
            arena: *std.heap.ArenaAllocator,
            args: std.ArrayList([:0]const u8),
            addresses: []std.net.Address,
        ) !void {
            const allocator = arena.allocator();

            var cmd: Command = .none;
            var rest = args.items;
            var interactive = false;

            for (args.items) |arg, i| {
                if (arg[0] == '-') {
                    continue;
                }

                if (std.mem.eql(u8, arg, "repl")) {
                    interactive = true;
                } else {
                    cmd = match_command(arg);
                }
                rest = args.items[i + 1 ..];

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

            if (interactive) {
                context.repl = true;
                try repl(arena, context);
            } else {
                try do_command(arena, rest, context, cmd);
            }

            while (!context.done) {
                context.client.tick();
                try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
        }

        fn parse_arg(
            comptime T: type,
            comptime name: []const u8,
            arg: []const u8,
            out: *T,
        ) !void {
            if (arg.len < name.len + 1 or !std.mem.eql(u8, arg[0 .. name.len + 1], name ++ ":")) {
                return;
            }

            out.* = try std.fmt.parseInt(T, arg[name.len + 1 ..], 10);
        }

        fn parse_account_flags(arg: []const u8, flags: *tb.AccountFlags) !void {
            flags.* = .{};

            if (!(arg.len > 6 and std.mem.eql(u8, arg[0..6], "flags:"))) {
                return;
            }

            var parts = std.mem.split(u8, arg[6..], "|");
            while (parts.next()) |flag| {
                if (std.mem.eql(u8, flag, "linked")) {
                    flags.*.linked = true;
                    continue;
                }

                if (std.mem.eql(u8, flag, "debits_must_not_exceed_credits")) {
                    flags.*.debits_must_not_exceed_credits = true;
                    continue;
                }

                if (std.mem.eql(u8, flag, "credits_must_not_exceed_debits")) {
                    flags.*.credits_must_not_exceed_debits = true;
                    continue;
                }

                panic("No such account flag: {s}.\n", .{flag});
            }
        }

        fn create_accounts(
            arena: *std.heap.ArenaAllocator,
            args: []const [:0]const u8,
            context: *Context,
        ) !void {
            var allocator = arena.allocator();
            var batch_accounts = try std.ArrayList(tb.Account).initCapacity(allocator, args.len);

            for (args) |arg| {
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

                var parts = std.mem.split(u8, arg, " ");
                while (parts.next()) |part| {
                    // Parse account fields
                    try parse_arg(u128, "id", part, &account.id);
                    try parse_arg(u128, "user_data", part, &account.user_data);
                    try parse_arg(u32, "ledger", part, &account.ledger);
                    try parse_arg(u16, "code", part, &account.code);
                    try parse_arg(u64, "debits_pending", part, &account.debits_pending);
                    try parse_arg(u64, "debits_posted", part, &account.debits_posted);
                    try parse_arg(u64, "credits_pending", part, &account.credits_pending);
                    try parse_arg(u64, "credits_posted", part, &account.credits_posted);

                    try parse_account_flags(part, &account.flags);
                }

                batch_accounts.appendAssumeCapacity(account);
            }

            assert(batch_accounts.items.len == args.len);

            // Submit batch.
            send(
                context,
                .create_accounts,
                std.mem.sliceAsBytes(batch_accounts.items),
            );
        }

        fn lookup_accounts(
            arena: *std.heap.ArenaAllocator,
            args: []const [:0]const u8,
            context: *Context,
        ) !void {
            var allocator = arena.allocator();
            var account_ids = try std.ArrayList(u128).initCapacity(allocator, 1);

            for (args) |arg| {
                var id: u128 = 0;
                try parse_arg(u128, "id", arg, &id);
                try account_ids.append(id);
            }

            if (account_ids.items.len == 0) {
                panic("Must pass at least one id. For example: `cli lookup-accounts id:12`.", .{});
            }

            // Submit batch.
            send(
                context,
                .lookup_accounts,
                std.mem.sliceAsBytes(account_ids.items),
            );
        }

        fn parse_transfer_flags(arg: []const u8, flags: *tb.TransferFlags) !void {
            flags.* = .{};

            if (!(arg.len > 6 and std.mem.eql(u8, arg[0..6], "flags:"))) {
                return;
            }

            var parts = std.mem.split(u8, arg[6..], "|");
            while (parts.next()) |flag| {
                if (std.mem.eql(u8, flag, "linked")) {
                    flags.*.linked = true;
                    continue;
                }

                if (std.mem.eql(u8, flag, "pending")) {
                    flags.*.pending = true;
                    continue;
                }

                if (std.mem.eql(u8, flag, "post_pending_transfer")) {
                    flags.*.post_pending_transfer = true;
                    continue;
                }

                if (std.mem.eql(u8, flag, "void_pending_transfer")) {
                    flags.*.void_pending_transfer = true;
                    continue;
                }

                if (std.mem.eql(u8, flag, "balancing_debit")) {
                    flags.*.balancing_debit = true;
                    continue;
                }

                if (std.mem.eql(u8, flag, "balancing_credit")) {
                    flags.*.balancing_credit = true;
                    continue;
                }

                panic("No such transfer flag: {s}.\n", .{flag});
            }
        }

        fn create_transfers(
            arena: *std.heap.ArenaAllocator,
            args: []const [:0]const u8,
            context: *Context,
        ) !void {
            var allocator = arena.allocator();
            var batch_transfers = try std.ArrayList(tb.Transfer).initCapacity(allocator, args.len);

            for (args) |arg| {
                var transfer = tb.Transfer{
                    .id = 0,
                    .debit_account_id = 0,
                    .credit_account_id = 0,
                    .user_data = 0,
                    .reserved = 0,
                    .pending_id = 0,
                    .timeout = 0,
                    .ledger = 0,
                    .code = 0,
                    .flags = .{},
                    .amount = 0,
                    .timestamp = 0,
                };

                var parts = std.mem.split(u8, arg, " ");
                while (parts.next()) |part| {
                    // Parse transfer fields
                    try parse_arg(u128, "id", part, &transfer.id);
                    try parse_arg(u128, "debit_account_id", part, &transfer.debit_account_id);
                    try parse_arg(u128, "credit_account_id", part, &transfer.credit_account_id);
                    try parse_arg(u128, "user_data", part, &transfer.user_data);
                    try parse_arg(u128, "pending_id", part, &transfer.pending_id);
                    try parse_arg(u64, "timeout", part, &transfer.timeout);
                    try parse_arg(u32, "ledger", part, &transfer.ledger);
                    try parse_arg(u16, "code", part, &transfer.code);
                    try parse_arg(u64, "amount", part, &transfer.amount);

                    try parse_transfer_flags(part, &transfer.flags);
                }

                batch_transfers.appendAssumeCapacity(transfer);
            }

            assert(batch_transfers.items.len == args.len);

            // Submit batch.
            send(
                context,
                .create_transfers,
                std.mem.sliceAsBytes(batch_transfers.items),
            );
        }

        fn lookup_transfers(
            arena: *std.heap.ArenaAllocator,
            args: []const [:0]const u8,
            context: *Context,
        ) !void {
            var allocator = arena.allocator();
            var transfer_ids = try std.ArrayList(u128).initCapacity(allocator, 1);

            for (args) |arg| {
                var id: u128 = 0;
                try parse_arg(u128, "id", arg, &id);
                try transfer_ids.append(id);
            }

            if (transfer_ids.items.len == 0) {
                panic("Must pass at least one id. For example: `cli lookup-transfers id:12`.", .{});
            }

            // Submit batch.
            send(
                context,
                .lookup_transfers,
                std.mem.sliceAsBytes(transfer_ids.items),
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

        fn display_account_flags(flags: tb.AccountFlags) !void {
            const stdout = std.io.getStdOut().writer();

            if (flags.linked) {
                try stdout.print("linked", .{});
            }

            if (flags.debits_must_not_exceed_credits) {
                if (flags.linked) {
                    try stdout.print("|", .{});
                }

                try stdout.print("debits_must_not_exceed_credits", .{});
            }

            if (flags.credits_must_not_exceed_debits) {
                if (flags.linked or flags.debits_must_not_exceed_credits) {
                    try stdout.print("|", .{});
                }

                try stdout.print("credits_must_not_exceed_debits", .{});
            }
        }

        fn display_accounts(accounts: []align(1) const tb.Account) void {
            const stdout = std.io.getStdOut().writer();

            for (accounts) |account| {
                stdout.print(
                    \\{{
                    \\  "id":              "{}",
                    \\  "user_data":       "{}",
                    \\  "ledger":          "{}",
                    \\  "code":            "{}",
                    \\  "flags":           "
                , .{
                    account.id,
                    account.user_data,
                    account.ledger,
                    account.code,
                }) catch unreachable;

                display_account_flags(account.flags) catch unreachable;

                stdout.print("\",\n", .{}) catch unreachable;

                stdout.print(
                    \\  "debits_pending":  "{}",
                    \\  "debits_posted":   "{}",
                    \\  "credits_pending": "{}",
                    \\  "credits_posted":  "{}"
                    \\}}
                    \\
                , .{
                    account.debits_pending,
                    account.debits_posted,
                    account.credits_pending,
                    account.credits_posted,
                }) catch unreachable;
            }
        }

        fn display_account_result_errors(errors: []align(1) const tb.CreateAccountsResult) void {
            const stdout = std.io.getStdOut().writer();

            for (errors) |reason| {
                stdout.print(
                    "Failed to create account ({}): {any}.\n",
                    .{ reason.index, reason.result },
                ) catch unreachable;
            }
        }

        fn display_transfer_flags(flags: tb.TransferFlags) !void {
            const stdout = std.io.getStdOut().writer();

            if (flags.linked) {
                try stdout.print("linked", .{});
            }

            if (flags.pending) {
                if (flags.linked) {
                    try stdout.print("|", .{});
                }

                try stdout.print("pending", .{});
            }

            if (flags.post_pending_transfer) {
                if (flags.linked or flags.pending) {
                    try stdout.print("|", .{});
                }

                try stdout.print("post_pending_transfer", .{});
            }

            if (flags.void_pending_transfer) {
                if (flags.linked or flags.pending or flags.post_pending_transfer) {
                    try stdout.print("|", .{});
                }

                try stdout.print("void_pending_transfer", .{});
            }

            if (flags.balancing_debit) {
                if (flags.linked or
                    flags.pending or
                    flags.post_pending_transfer or
                    flags.void_pending_transfer)
                {
                    try stdout.print("|", .{});
                }

                try stdout.print("balancing_debit", .{});
            }

            if (flags.balancing_credit) {
                if (flags.linked or
                    flags.pending or
                    flags.post_pending_transfer or
                    flags.void_pending_transfer or
                    flags.balancing_debit)
                {
                    try stdout.print("|", .{});
                }

                try stdout.print("balancing_credit", .{});
            }
        }

        fn display_transfers(transfers: []align(1) const tb.Transfer) void {
            const stdout = std.io.getStdOut().writer();

            for (transfers) |transfer| {
                stdout.print(
                    \\{{
                    \\  "id":                "{}",
                    \\  "debit_account_id":  "{}",
                    \\  "credit_account_id": "{}",
                    \\  "user_data":         "{}",
                    \\  "pending_id":        "{}",
                    \\  "timeout":           "{}",
                    \\  "ledger":            "{}",
                    \\  "code":              "{}",
                    \\  "flags":             "
                , .{
                    transfer.id,
                    transfer.debit_account_id,
                    transfer.credit_account_id,
                    transfer.user_data,
                    transfer.pending_id,
                    transfer.timeout,
                    transfer.ledger,
                    transfer.code,
                }) catch unreachable;

                display_transfer_flags(transfer.flags) catch unreachable;

                stdout.print("\",\n", .{}) catch unreachable;

                stdout.print(
                    \\  "amount":            "{}",
                    \\  "timestamp":         "{}",
                    \\}}
                    \\
                , .{
                    transfer.amount,
                    transfer.timestamp,
                }) catch unreachable;
            }
        }

        fn display_transfer_result_errors(errors: []align(1) const tb.CreateTransfersResult) void {
            const stdout = std.io.getStdOut().writer();

            for (errors) |reason| {
                stdout.print(
                    "Failed to create transfer ({}): {any}.\n",
                    .{ reason.index, reason.result },
                ) catch unreachable;
            }
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
                        display_account_result_errors(create_account_results);
                    }
                },
                .lookup_accounts => {
                    const lookup_account_results = std.mem.bytesAsSlice(
                        tb.Account,
                        result_payload,
                    );

                    if (lookup_account_results.len == 0) {
                        panic("No such account exists.\n", .{});
                    }

                    display_accounts(lookup_account_results);
                },
                .create_transfers => {
                    const create_transfer_results = std.mem.bytesAsSlice(
                        tb.CreateTransfersResult,
                        result_payload,
                    );

                    if (create_transfer_results.len > 0) {
                        display_transfer_result_errors(create_transfer_results);
                    }
                },
                .lookup_transfers => {
                    const lookup_transfer_results = std.mem.bytesAsSlice(
                        tb.Transfer,
                        result_payload,
                    );

                    if (lookup_transfer_results.len == 0) {
                        panic("No such transfer exists.\n", .{});
                    }

                    display_transfers(lookup_transfer_results);
                },
                else => unreachable,
            }

            const context = @intToPtr(*Context, @intCast(u64, user_data));

            context.client.unref(context.message.?);
            context.message = null;

            if (!context.repl) {
                context.done = true;
            }
        }
    };
}
