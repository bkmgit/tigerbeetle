const std = @import("std");

const alloc = std.testing.allocator;

const vsr = @import("vsr");
const constants = vsr.constants;
const tb = vsr.tigerbeetle;
const MessageBus = vsr.message_bus.MessageBusReplica;
const Storage = vsr.storage.Storage;
const StateMachine = vsr.state_machine.StateMachineType(Storage, constants.state_machine_config);
const VSRClient = vsr.Client(StateMachine, MessageBus);

const client = @import("./client.zig");
const Client = client.ClientType(StateMachine, MessageBus);

test "Parsing transfers successfully" {
    var context = try alloc.create(Client.Context);
    defer alloc.destroy(context);

    var tests = [_]struct {
        in: []const u8,
        want: tb.Transfer,
    }{
        .{
            .in = "id:1",
            .want = tb.Transfer{
                .id = 1,
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
            },
        },
        .{
            .in = "id:32 amount:65 ledger:12 code:9999 pending_id:7 credit_account_id:2121 debit_account_id:77 user_data:2 flags:linked",
            .want = tb.Transfer{
                .id = 32,
                .debit_account_id = 77,
                .credit_account_id = 2121,
                .user_data = 2,
                .reserved = 0,
                .pending_id = 7,
                .timeout = 0,
                .ledger = 12,
                .code = 9999,
                .flags = .{ .linked = true },
                .amount = 65,
                .timestamp = 0,
            },
        },
        .{
            .in = "flags:post_pending_transfer|balancing_credit|balancing_debit|void_pending_transfer|pending|linked",
            .want = tb.Transfer{
                .id = 0,
                .debit_account_id = 0,
                .credit_account_id = 0,
                .user_data = 0,
                .reserved = 0,
                .pending_id = 0,
                .timeout = 0,
                .ledger = 0,
                .code = 0,
                .flags = .{
                    .post_pending_transfer = true,
                    .balancing_credit = true,
                    .balancing_debit = true,
                    .void_pending_transfer = true,
                    .pending = true,
                    .linked = true,
                },
                .amount = 0,
                .timestamp = 0,
            },
        },
    };

    for (tests) |t| {
        var res = Client.parse_transfer(
            context,
            t.in,
        );
        try std.testing.expectEqual(res, t.want);
    }
}

test "Parsing accounts successfully" {
    var context = try alloc.create(Client.Context);
    defer alloc.destroy(context);

    var tests = [_]struct {
        in: []const u8,
        want: tb.Account,
    }{
        .{
            .in = "id:1",
            .want = tb.Account{
                .id = 1,
                .user_data = 0,
                .reserved = [_]u8{0} ** 48,
                .ledger = 0,
                .code = 0,
                .flags = .{},
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
            },
        },
        .{
            .in = "id:32 credits_posted:344 ledger:12 credits_pending:18 code:9999 debits_posted:3390 debits_pending:3212 user_data:2 flags:linked",
            .want = tb.Account{
                .id = 32,
                .user_data = 2,
                .reserved = [_]u8{0} ** 48,
                .ledger = 12,
                .code = 9999,
                .flags = .{ .linked = true },
                .debits_pending = 3212,
                .debits_posted = 3390,
                .credits_pending = 18,
                .credits_posted = 344,
            },
        },
        .{
            .in = "flags:credits_must_not_exceed_debits|linked|debits_must_not_exceed_credits",
            .want = tb.Account{
                .id = 0,
                .user_data = 0,
                .reserved = [_]u8{0} ** 48,
                .ledger = 0,
                .code = 0,
                .debits_pending = 0,
                .debits_posted = 0,
                .credits_pending = 0,
                .credits_posted = 0,
                .flags = .{
                    .credits_must_not_exceed_debits = true,
                    .linked = true,
                    .debits_must_not_exceed_credits = true,
                },
            },
        },
    };

    for (tests) |t| {
        var res = Client.parse_account(
            context,
            t.in,
        );
        try std.testing.expectEqual(res, t.want);
    }
}
