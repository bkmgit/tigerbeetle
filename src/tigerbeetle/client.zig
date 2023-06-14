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
        const Self = @This();
        const Client = vsr.Client(StateMachine, MessageBus);

        fn err(comptime m: []const u8, args: anytype) noreturn {
            const stderr = std.io.getStdErr().writer();
            stderr.print(m, args) catch unreachable;
            std.os.exit(1);
        }

        fn print(comptime m: []const u8, args: anytype) void {
            const stdout = std.io.getStdOut().writer();
            stdout.print(m, args) catch return;
        }

        pub const Context = struct {
            event_loop_done: bool,
            request_done: bool,

            repl: bool,
            debug_logs: bool,

            client: *Client,
            message: ?*MessagePool.Message,

            fn err(context: *Context, comptime m: []const u8, args: anytype) void {
                if (!context.repl) {
                    Self.err(m, args);
                }

                print(m, args);
            }

            fn debug(context: *Context, comptime m: []const u8, args: anytype) void {
                if (context.debug_logs) {
                    print("[Debug] " ++ m, args);
                }
            }
        };

        pub const Command = enum {
            none,
            create_accounts,
            lookup_accounts,
            create_transfers,
            lookup_transfers,
        };

        pub const LookupST = struct {
            id: u128,
        };

        pub const ObjectST = union(enum) {
            account: tb.Account,
            transfer: tb.Transfer,
            id: LookupST,
        };

        pub const StatementST = struct {
            cmd: Command,
            args: []ObjectST,
        };

        fn eat_whitespace(input: []const u8, initial_index: usize) usize {
            var index = initial_index;
            while (index < input.len and std.ascii.isSpace(input[index])) {
                index += 1;
            }

            return index;
        }

        const ParseIdentifierResult = struct {
            string: []const u8,
            next_i: usize,
        };
        fn parse_identifier(input: []const u8, initial_index: usize) !ParseIdentifierResult {
            var index = eat_whitespace(input, initial_index);

            while (index < input.len and (std.ascii.isAlpha(input[index]) or input[index] == '_')) {
                index += 1;
            }

            return ParseIdentifierResult{
                .string = input[initial_index..index],
                .next_i = index,
            };
        }

        fn parse_syntax(input: []const u8, initial_index: usize, syntax: u8) !usize {
            var index = eat_whitespace(input, initial_index);
            if (index >= input.len) {
                return index;
            }

            if (input[index] == syntax) {
                return index + 1;
            }

            return error.NoSyntaxMatch;
        }

        const ParseValueResult = struct {
            string: []const u8,
            next_i: usize,
        };
        fn parse_value(
            input: []const u8,
            initial_index: usize,
        ) !ParseValueResult {
            var index = eat_whitespace(input, initial_index);

            while (index < input.len) {
                const c = input[index];
                if (!(std.ascii.isAlNum(c) or c == '_' or c == '|')) {
                    break;
                }

                index += 1;
            }

            return ParseValueResult{
                .string = input[initial_index..index],
                .next_i = index + 1,
            };
        }

        fn match_arg(
            out: *ObjectST,
            key: []const u8,
            value: []const u8,
        ) !void {
            inline for (@typeInfo(ObjectST).Union.fields) |enum_field| {
                if (std.mem.eql(u8, @tagName(out.*), enum_field.name)) {
                    var sub = @field(out, enum_field.name);
                    const T = @TypeOf(sub);

                    switch (@typeInfo(T)) {
                        .Struct => |structInfo| {
                            inline for (structInfo.fields) |field| {
                                if (std.mem.eql(u8, field.name, key)) {
                                    // Handle everything but flags, skip reserved and timestamp.
                                    if (comptime (!std.mem.eql(u8, field.name, "flags") and
                                        !std.mem.eql(u8, field.name, "reserved") and
                                        !std.mem.eql(u8, field.name, "timestamp")))
                                    {
                                        @field(@field(out.*, enum_field.name), field.name) = try std.fmt.parseInt(field.field_type, value, 10);
                                    }
                                }
                            }
                        },

                        else => unreachable,
                    }

                    // Handle flags, specific to Account and Transfer fields.
                    if (comptime !std.mem.eql(u8, enum_field.name, "id")) {
                        if (std.mem.eql(u8, key, "flags")) {
                            var flags = std.mem.split(u8, value, "|");

                            const FlagT = @TypeOf(@field(@field(out.*, enum_field.name), "flags"));
                            var f = std.mem.zeroInit(FlagT, .{});
                            while (flags.next()) |flag| {
                                inline for (@typeInfo(FlagT).Struct.fields) |field| {
                                    if (std.mem.eql(u8, field.name, flag)) {
                                        if (comptime !std.mem.eql(u8, field.name, "padding")) {
                                            @field(f, field.name) = true;
                                        }
                                    }
                                }
                            }

                            @field(@field(out.*, enum_field.name), "flags") = f;
                        }
                    }
                }
            }
        }

        // Statement grammar parsed here.
        //  STMT: CMD ARGS [;]
        //   CMD: create_accounts | lookup_accounts | create_transfers | lookup_transfers
        //  ARGS: ARG [, ARG]
        //   ARG: KEY = VALUE
        //   KEY: string
        // VALUE: string [| VALUE]
        //
        // For example:
        //   create_accounts id=1 code=2 ledger=3, id = 2 code= 2 ledger =3;
        //   create_accounts flags=linked|debits_must_not_exceed_credits;
        pub fn parse_statement(
            context: *Context,
            arena: *std.heap.ArenaAllocator,
            input: []const u8,
        ) !StatementST {
            var args = std.ArrayList(ObjectST).init(arena.allocator());

            var i: usize = 0;
            var id_result = try parse_identifier(input, i);
            i = id_result.next_i;

            var cmd: Command = .none;
            if (std.mem.eql(u8, id_result.string, "create_accounts")) {
                cmd = .create_accounts;
            } else if (std.mem.eql(u8, id_result.string, "lookup_accounts")) {
                cmd = .lookup_accounts;
            } else if (std.mem.eql(u8, id_result.string, "create_transfers")) {
                cmd = .create_transfers;
            } else if (std.mem.eql(u8, id_result.string, "lookup_transfers")) {
                cmd = .lookup_transfers;
            } else {
                context.err(
                    "Command must be create_accounts, lookup_accounts, create_transfers, or lookup_transfers. Got: '{s}'.\n",
                    .{id_result.string},
                );
                return error.BadCommand;
            }

            var default = ObjectST{ .id = .{ .id = 0 } };
            if (cmd == .create_accounts) {
                default = ObjectST{ .account = std.mem.zeroInit(tb.Account, .{}) };
            } else if (cmd == .create_transfers) {
                default = ObjectST{ .transfer = std.mem.zeroInit(tb.Transfer, .{}) };
            }
            var object = default;

            var has_fields = false;
            while (i < input.len) {
                i = eat_whitespace(input, i);
                // Always need to check `i` against length in case we've hit the end.
                if (i >= input.len or input[i] == ';') {
                    break;
                }

                // Expect , separating key-value pairs
                if (i >= input.len or input[i] == ',') {
                    i = parse_syntax(input, i, ',') catch |e| {
                        context.err("Could not find , separating key-value pairs near {}.\n", .{i});
                        return e;
                    };

                    var copy = try arena.allocator().create(ObjectST);
                    copy.* = object;
                    context.debug("Found object: {any}.\n", .{copy.*});
                    try args.append(copy.*);

                    // Reset object.
                    object = default;
                    has_fields = false;
                }

                // Grab key
                id_result = try parse_identifier(input, i);
                i = id_result.next_i;

                // Grab =
                i = parse_syntax(input, i, '=') catch |e| {
                    context.err("Could not find = in key-value pair at {}.\n", .{i});
                    return e;
                };

                // Grab value
                var value_result = try parse_value(input, i);
                i = value_result.next_i;

                match_arg(&object, id_result.string, value_result.string) catch |e| {
                    context.err(
                        "'{s}'='{s}' is not a valid pair for {s}.",
                        .{ id_result.string, value_result.string, @tagName(object) },
                    );
                    return e;
                };
                context.debug(
                    "Set {s}.{s} = {s}.\n",
                    .{ @tagName(object), id_result.string, value_result.string },
                );

                has_fields = true;
            }

            // Add final object
            if (has_fields) {
                var copy = try arena.allocator().create(ObjectST);
                copy.* = object;
                context.debug("Found object: {any}.\n", .{copy.*});
                try args.append(copy.*);
            }

            return StatementST{
                .cmd = cmd,
                .args = args.items,
            };
        }

        fn do_statement(
            context: *Context,
            arena: *std.heap.ArenaAllocator,
            stmt: StatementST,
        ) !void {
            context.debug("Running command: {}.\n", .{stmt.cmd});
            switch (stmt.cmd) {
                .create_accounts => try create(tb.Account, "account", context, arena, stmt.args),
                .lookup_accounts => try lookup("account", context, arena, stmt.args),
                .create_transfers => try create(tb.Transfer, "transfer", context, arena, stmt.args),
                .lookup_transfers => try lookup("transfer", context, arena, stmt.args),
                else => err("Command not yet implemented.", .{}),
            }
        }

        fn repl(
            context: *Context,
            arena: *std.heap.ArenaAllocator,
        ) !void {
            print("> ", .{});

            const in = std.io.getStdIn();
            var stream = std.io.bufferedReader(in.reader()).reader();

            var input = std.ArrayList(u8).init(arena.allocator());
            var buf: [4096]u8 = undefined;

            if (stream.readUntilDelimiterOrEof(&buf, ';')) |bytes| {
                if (bytes) |b| {
                    try input.appendSlice(b);
                } else {
                    // EOF
                    context.event_loop_done = true;
                    context.err("\nExiting.\n", .{});
                    return;
                }
            } else |e| {
                context.event_loop_done = true;
                err("Failed to read from stdin: {any}\n", .{e});
                return e;
            }

            var stmt = parse_statement(context, arena, input.items) catch return;

            // No input was parsed.
            if (stmt.cmd == .none) {
                context.debug("No command was parsed, continuing.\n", .{});
                return;
            }

            try do_statement(
                context,
                arena,
                stmt,
            );
        }

        fn display_help() void {
            print(
                \\TigerBeetle Client
                \\  Terminal client for interacting with TigerBeetle.
                \\
                \\  Flags:
                \\    --addresses=address[,...]  Specify TigerBeetle replica addresses.
                \\    --help, -h                 Print this message.
                \\    --debug, -d                Show additional debugging logs.
                \\
                \\  Commands:
                \\    create_accounts            Create one or more accounts grouped by quotes, separated by spaces.
                \\    lookup_accounts            Look up one or more accounts separated by spaces.
                \\    create_transfers           Create one or more transfers grouped by quotes, separated by spaces.
                \\    lookup_transfers           Look up one or more transfers separated by spaces.
                \\    repl                       Enter an interactive REPL.
                \\
                \\Examples:
                \\  $ tigerbeetle client --addresses=3000 create_accounts \
                \\    "id:1 code:1 ledger:1" \
                \\    "id:2 code:1 ledger:1"
                \\  $ tigerbeetle client --addresses=3000 create_transfers \
                \\    "id:1 debit_account_id:1 credit_account_id:2 amount:10 ledger:1 code:1"
                \\  $ tigerbeetle client --addresses=3000 lookup_accounts "id:1"
                \\  $ tigerbeetle client --addresses=3000 repl
            , .{});
        }

        pub fn run(
            arena: *std.heap.ArenaAllocator,
            args: std.ArrayList([:0]const u8),
            addresses: []std.net.Address,
        ) !void {
            const allocator = arena.allocator();

            var debug = false;
            var statements: ?[]const u8 = null;

            for (args.items) |arg| {
                if (arg[0] == '-') {
                    if (std.mem.eql(u8, arg, "--debug") or
                        std.mem.eql(u8, arg, "-d"))
                    {
                        debug = true;
                    } else if (std.mem.eql(u8, arg, "--help") or
                        std.mem.eql(u8, arg, "-h"))
                    {
                        display_help();
                        return;
                    } else if (std.mem.startsWith(u8, arg, "--addresses=")) {
                        // Already handled by ./cli.zig
                    } else if (std.mem.startsWith(u8, arg, "--command=")) {
                        statements = arg["--command=".len..];
                    } else {
                        err("Unexpected argument: '{s}'.\n", .{arg});
                    }

                    continue;
                }
            }

            var context = try allocator.create(Context);
            context.debug_logs = debug;
            context.request_done = true;
            context.event_loop_done = false;
            context.repl = statements == null;

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

            if (statements) |stmts_| {
                var stmts = std.mem.split(u8, stmts_, ";");
                while (stmts.next()) |stmt_string| {
                    // Gets reset after every execution.
                    var execution_arena = &std.heap.ArenaAllocator.init(arena.allocator());
                    defer execution_arena.deinit();
                    var stmt = parse_statement(context, execution_arena, stmt_string) catch return;
                    do_statement(context, execution_arena, stmt) catch return;
                }
            } else {
                print(
                    \\TigerBeetle Client
                    \\  Hit enter after a semicolon to run a command.
                    \\
                    \\Examples:
                    \\  create_accounts id=1 code=1 ledger=1,
                    \\                  id=2 code=1 ledger=1;
                    \\  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=1 code=1;
                    \\  lookup_accounts id=1;
                    \\  lookup_accounts id=1, id=2;
                    \\
                    \\
                , .{});
            }

            while (!context.event_loop_done) {
                if (context.request_done and context.repl) {
                    // Gets reset after every execution.
                    var execution_arena = &std.heap.ArenaAllocator.init(arena.allocator());
                    defer execution_arena.deinit();
                    repl(context, execution_arena) catch return;
                }
                context.client.tick();
                try io.run_for_ns(constants.tick_ms * std.time.ns_per_ms);
            }
        }

        fn create(
            comptime T: type,
            comptime name: []const u8,
            context: *Context,
            arena: *std.heap.ArenaAllocator,
            objects: []ObjectST,
        ) !void {
            if (objects.len == 0) {
                context.err("No " ++ name ++ "s to create.\n", .{});
                return;
            }

            var allocator = arena.allocator();
            var batch = try std.ArrayList(T).initCapacity(allocator, objects.len);

            for (objects) |object| {
                batch.appendAssumeCapacity(@field(object, name));
            }

            assert(batch.items.len == objects.len);

            // Submit batch.
            send(
                context,
                if (std.mem.eql(u8, name, "account")) .create_accounts else .create_transfers,
                std.mem.sliceAsBytes(batch.items),
            );
        }

        fn lookup(
            comptime t: []const u8,
            context: *Context,
            arena: *std.heap.ArenaAllocator,
            objects: []ObjectST,
        ) !void {
            if (objects.len == 0) {
                context.err("No " ++ t ++ "s to look up.\n", .{});
                return;
            }

            var allocator = arena.allocator();
            var ids = try std.ArrayList(u128).initCapacity(allocator, objects.len);

            for (objects) |object| {
                try ids.append(object.id.id);
            }

            // Submit batch.
            send(
                context,
                if (std.mem.eql(u8, t, "account")) .lookup_accounts else .lookup_transfers,
                std.mem.sliceAsBytes(ids.items),
            );
        }

        fn send(
            context: *Context,
            operation: StateMachine.Operation,
            payload: []u8,
        ) void {
            context.request_done = false;
            context.message = context.client.get_message();

            stdx.copy_disjoint(
                .inexact,
                u8,
                context.message.?.buffer[@sizeOf(vsr.Header)..],
                payload,
            );

            context.debug("Sending command: {}.\n", .{operation});
            context.client.request(
                @intCast(u128, @ptrToInt(context)),
                send_complete,
                operation,
                context.message.?,
                payload.len,
            );
        }

        fn display_account_flags(flags: tb.AccountFlags) void {
            if (flags.linked) {
                print("linked", .{});
            }

            if (flags.debits_must_not_exceed_credits) {
                if (flags.linked) {
                    print("|", .{});
                }

                print("debits_must_not_exceed_credits", .{});
            }

            if (flags.credits_must_not_exceed_debits) {
                if (flags.linked or flags.debits_must_not_exceed_credits) {
                    print("|", .{});
                }

                print("credits_must_not_exceed_debits", .{});
            }
        }

        fn display_accounts(accounts: []align(1) const tb.Account) void {
            for (accounts) |account| {
                print(
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
                });

                display_account_flags(account.flags);

                print("\",\n", .{});

                print(
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
                });
            }
        }

        fn display_account_result_errors(errors: []align(1) const tb.CreateAccountsResult) void {
            for (errors) |reason| {
                print(
                    "Failed to create account ({}): {any}.\n",
                    .{ reason.index, reason.result },
                );
            }
        }

        fn display_transfer_flags(flags: tb.TransferFlags) void {
            if (flags.linked) {
                print("linked", .{});
            }

            if (flags.pending) {
                if (flags.linked) {
                    print("|", .{});
                }

                print("pending", .{});
            }

            if (flags.post_pending_transfer) {
                if (flags.linked or flags.pending) {
                    print("|", .{});
                }

                print("post_pending_transfer", .{});
            }

            if (flags.void_pending_transfer) {
                if (flags.linked or flags.pending or flags.post_pending_transfer) {
                    print("|", .{});
                }

                print("void_pending_transfer", .{});
            }

            if (flags.balancing_debit) {
                if (flags.linked or
                    flags.pending or
                    flags.post_pending_transfer or
                    flags.void_pending_transfer)
                {
                    print("|", .{});
                }

                print("balancing_debit", .{});
            }

            if (flags.balancing_credit) {
                if (flags.linked or
                    flags.pending or
                    flags.post_pending_transfer or
                    flags.void_pending_transfer or
                    flags.balancing_debit)
                {
                    print("|", .{});
                }

                print("balancing_credit", .{});
            }
        }

        fn display_transfers(transfers: []align(1) const tb.Transfer) void {
            for (transfers) |transfer| {
                print(
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
                });

                display_transfer_flags(transfer.flags);

                print("\",\n", .{});

                print(
                    \\  "amount":            "{}",
                    \\  "timestamp":         "{}",
                    \\}}
                    \\
                , .{
                    transfer.amount,
                    transfer.timestamp,
                });
            }
        }

        fn display_transfer_result_errors(errors: []align(1) const tb.CreateTransfersResult) void {
            for (errors) |reason| {
                print(
                    "Failed to create transfer ({}): {any}.\n",
                    .{ reason.index, reason.result },
                );
            }
        }

        fn send_complete(
            user_data: u128,
            operation: StateMachine.Operation,
            result: Client.Error![]const u8,
        ) void {
            const context = @intToPtr(*Context, @intCast(u64, user_data));
            context.debug("Command completed: {}.\n", .{operation});

            defer {
                context.request_done = true;
                context.client.unref(context.message.?);
                context.message = null;

                if (!context.repl) {
                    context.event_loop_done = true;
                }
            }

            const result_payload = result catch |err| {
                context.err("Client returned error: {}", .{err});
                return;
            };

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
                        context.err("No such account exists.\n", .{});
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
                        context.err("No such transfer exists.\n", .{});
                    }

                    display_transfers(lookup_transfer_results);
                },
                else => unreachable,
            }
        }
    };
}
