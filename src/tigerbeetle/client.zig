const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");

const build_options = @import("vsr_options");

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

            fn err_at(
                context: *Context,
                input: []const u8,
                index: usize,
                comptime m: []const u8,
                args: anytype,
            ) void {
                var line_no: usize = 1;
                var col_no: usize = 0;

                var line_beginning: usize = 0;
                var found_current_line = false;

                var i: usize = 0;
                while (i < input.len) : (i += 1) {
                    var c = input[i];

                    if (c == '\n') {
                        if (!found_current_line) {
                            line_beginning = i + 1;
                            line_no += 1;
                            col_no = 0;
                        } else {
                            break;
                        }
                    } else {
                        if (!found_current_line) {
                            col_no += 1;
                        }
                    }

                    if (i == index) {
                        found_current_line = true;
                    }
                }

                var line = input[line_beginning..i];

                const stderr = std.io.getStdErr().writer();
                stderr.print("Error near line {}, column {}:\n\n{s}\n", .{ line_no, col_no, line }) catch unreachable;
                while (col_no > 0) {
                    stderr.print(" ", .{}) catch unreachable;
                    col_no -= 1;
                }
                stderr.print("^ Near here.\n\n", .{}) catch unreachable;

                context.err(m, args);
            }
        };

        pub const Command = ?StateMachine.Operation;

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
            next_index: usize,
        };
        fn parse_identifier(input: []const u8, initial_index: usize) !ParseIdentifierResult {
            var index = eat_whitespace(input, initial_index);
            var after_whitespace = index;

            while (index < input.len and (std.ascii.isAlpha(input[index]) or input[index] == '_')) {
                index += 1;
            }

            return ParseIdentifierResult{
                .string = input[after_whitespace..index],
                .next_index = index,
            };
        }

        fn parse_syntax(input: []const u8, initial_index: usize, syntax: u8) !usize {
            var index = eat_whitespace(input, initial_index);
            if (index < input.len and input[index] == syntax) {
                return index + 1;
            }

            return error.NoSyntaxMatch;
        }

        const ParseValueResult = struct {
            string: []const u8,
            next_index: usize,
        };
        fn parse_value(
            input: []const u8,
            initial_index: usize,
        ) !ParseValueResult {
            var index = eat_whitespace(input, initial_index);
            var after_whitespace = index;

            while (index < input.len) {
                const c = input[index];
                if (!(std.ascii.isAlNum(c) or c == '_' or c == '|')) {
                    break;
                }

                index += 1;
            }

            return ParseValueResult{
                .string = input[after_whitespace..index],
                .next_index = index,
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
                    const SubT = @TypeOf(sub);

                    inline for (@typeInfo(SubT).Struct.fields) |field| {
                        if (std.mem.eql(u8, field.name, key)) {
                            // Handle everything but flags, skip reserved and timestamp.
                            if (comptime (!std.mem.eql(u8, field.name, "flags") and
                                !std.mem.eql(u8, field.name, "reserved") and
                                !std.mem.eql(u8, field.name, "timestamp")))
                            {
                                @field(@field(out.*, enum_field.name), field.name) = try std.fmt.parseInt(field.field_type, value, 10);
                            }

                            // Handle flags, specific to Account and Transfer fields.
                            if (comptime std.mem.eql(u8, field.name, "flags") and !std.mem.eql(u8, enum_field.name, "id")) {
                                var flags = std.mem.split(u8, value, "|");
                                var f = std.mem.zeroInit(field.field_type, .{});
                                while (flags.next()) |flag| {
                                    inline for (@typeInfo(field.field_type).Struct.fields) |flag_field| {
                                        if (std.mem.eql(u8, flag_field.name, flag)) {
                                            if (comptime !std.mem.eql(u8, flag_field.name, "padding")) {
                                                @field(f, flag_field.name) = true;
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

            var after_whitespace: usize = eat_whitespace(input, 0);
            var id_result = try parse_identifier(input, after_whitespace);
            var i = id_result.next_index;

            var cmd: Command = null;
            if (std.mem.eql(u8, id_result.string, "help")) {
                display_help();
                return error.Help;
            } else if (std.mem.eql(u8, id_result.string, "create_accounts")) {
                cmd = .create_accounts;
            } else if (std.mem.eql(u8, id_result.string, "lookup_accounts")) {
                cmd = .lookup_accounts;
            } else if (std.mem.eql(u8, id_result.string, "create_transfers")) {
                cmd = .create_transfers;
            } else if (std.mem.eql(u8, id_result.string, "lookup_transfers")) {
                cmd = .lookup_transfers;
            } else {
                context.err_at(
                    input,
                    after_whitespace,
                    "Command must be help, create_accounts, lookup_accounts, create_transfers, or lookup_transfers. Got: '{s}'.\n",
                    .{id_result.string},
                );
                return error.BadCommand;
            }

            var default = ObjectST{ .id = .{ .id = 0 } };
            if (cmd) |c| {
                if (c == .create_accounts) {
                    default = ObjectST{ .account = std.mem.zeroInit(tb.Account, .{}) };
                } else if (c == .create_transfers) {
                    default = ObjectST{ .transfer = std.mem.zeroInit(tb.Transfer, .{}) };
                }
            } else {
                unreachable;
            }
            var object = default;

            var has_fields = false;
            while (i < input.len) {
                i = eat_whitespace(input, i);
                // Always need to check `i` against length in case we've hit the end.
                if (i >= input.len or input[i] == ';') {
                    break;
                }

                // Expect comma separating objects
                if (i < input.len and input[i] == ',') {
                    i += 1;

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
                i = id_result.next_index;

                if (id_result.string.len == 0) {
                    context.err_at(input, i, "Expected key starting key-value pair. e.g. `id=1`\n", .{});
                    return error.BadIdentifier;
                }

                // Grab =
                i = parse_syntax(input, i, '=') catch |e| {
                    context.err_at(input, i, "Expected equal sign after key in key-value pair: {any}. e.g. `id=1`.\n", .{e});
                    return error.MissingEqualBetweenKeyValuePair;
                };

                // Grab value
                var value_result = try parse_value(input, i);
                i = value_result.next_index;

                if (value_result.string.len == 0) {
                    context.err_at(input, i, "Expected value after equal sign in key-value pair. e.g. `id=1`.\n", .{});
                    return error.BadValue;
                }

                // Match key to a field in the struct.
                match_arg(&object, id_result.string, value_result.string) catch |e| {
                    context.err_at(
                        input,
                        i,
                        "'{s}'='{s}' is not a valid pair for {s}: {any}.",
                        .{ id_result.string, value_result.string, @tagName(object), e },
                    );
                    return error.BadKeyValuePair;
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
                context.debug("Found final object: {any}.\n", .{copy.*});
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
            if (stmt.cmd) |cmd| {
                context.debug("Running command: {}.\n", .{cmd});
                switch (cmd) {
                    .create_accounts => try create(tb.Account, "account", context, arena, stmt.args),
                    .lookup_accounts => try lookup("account", context, arena, stmt.args),
                    .create_transfers => try create(tb.Transfer, "transfer", context, arena, stmt.args),
                    .lookup_transfers => try lookup("transfer", context, arena, stmt.args),
                }
                return;
            }

            // No input was parsed.
            context.debug("No command was parsed, continuing.\n", .{});
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
            try do_statement(
                context,
                arena,
                stmt,
            );
        }

        fn display_help() void {
            const version = build_options.git_tag orelse "experimental";
            print("TigerBeetle CLI Client " ++ version ++ "\n" ++
                \\  Hit enter after a semicolon to run a command.
                \\
                \\Examples:
                \\  create_accounts id=1 code=10 ledger=700,
                \\                  id=2 code=10 ledger=700;
                \\  create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10;
                \\  lookup_accounts id=1;
                \\  lookup_accounts id=1, id=2;
                \\
                \\
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
                    if (std.mem.eql(u8, arg, "--debug")) {
                        debug = true;
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
                display_help();
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

        fn display_accounts(accounts: []align(1) const tb.Account) void {
            for (accounts) |account| {
                display_object(account);
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

        fn display_object(object: anytype) void {
            const T = @TypeOf(object);
            print("{{\n", .{});
            inline for (@typeInfo(T).Struct.fields) |s_field, i| {
                if (comptime std.mem.eql(u8, s_field.name, "reserved")) {
                    continue;
                    // No need to print out reserved.
                }

                if (i > 0) {
                    print(",\n", .{});
                }

                if (comptime std.mem.eql(u8, s_field.name, "flags")) {
                    print("  \"" ++ s_field.name ++ "\": [", .{});
                    var needs_comma = false;

                    inline for (@typeInfo(s_field.field_type).Struct.fields) |flag_field| {
                        if (comptime !std.mem.eql(u8, flag_field.name, "padding")) {
                            if (@field(@field(object, "flags"), flag_field.name)) {
                                if (needs_comma) {
                                    print(",", .{});
                                    needs_comma = false;
                                }

                                print("\"" ++ flag_field.name ++ "\"", .{});
                                needs_comma = true;
                            }
                        }
                    }

                    print("]", .{});
                } else {
                    print("  \"" ++ s_field.name ++ "\": \"{}\"", .{@field(object, s_field.name)});
                }
            }
            print("\n}}\n", .{});
        }

        fn display_transfers(transfers: []align(1) const tb.Transfer) void {
            for (transfers) |transfer| {
                display_object(transfer);
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
                    } else {
                        for (lookup_account_results) |account| {
                            display_object(account);
                        }
                    }
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
                    } else {
                        for (lookup_transfer_results) |transfer| {
                            display_object(transfer);
                        }
                    }
                },
            }
        }
    };
}
