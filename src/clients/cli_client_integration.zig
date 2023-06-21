const std = @import("std");
const builtin = @import("builtin");

const TmpDir = @import("./shutil.zig").TmpDir;
const read_file = @import("./shutil.zig").read_file;
const shell_wrap = @import("./shutil.zig").shell_wrap;
const run_shell = @import("./shutil.zig").run_shell;
const binary_filename = @import("./shutil.zig").binary_filename;
const run_many_with_tb = @import("./run_with_tb.zig").run_many_with_tb;

const tb_client_command_base =
    "{s} client --addresses=$" ++
    (if (builtin.os.tag == .windows) "env:") ++
    "TB_ADDRESS --command=\"{s}\"";

fn tb_client_command(
    arena: *std.heap.ArenaAllocator,
    tb_binary: []const u8,
    command: []const u8,
) ![]const []const u8 {
    return try shell_wrap(
        arena,
        try std.fmt.allocPrint(
            arena.allocator(),
            tb_client_command_base,
            .{ tb_binary, command },
        ),
    );
}

fn tb_client_command_json_out(
    arena: *std.heap.ArenaAllocator,
    tb_binary: []const u8,
    command: []const u8,
    out_name: []const u8,
) ![]const []const u8 {
    return try shell_wrap(
        arena,
        try std.fmt.allocPrint(
            arena.allocator(),
            tb_client_command_base ++ " | jq \"del(.timestamp)\" > {s}",
            .{ tb_binary, command, out_name },
        ),
    );
}

fn fail_on_diff(
    arena: *std.heap.ArenaAllocator,
    tmp_dir: []const u8,
    expected: []const u8,
    out_name: []const u8,
) !void {
    var out_file = try read_file(
        arena,
        try std.fmt.allocPrint(arena.allocator(), "{s}/{s}", .{ tmp_dir, out_name }),
    );
    const wanted = std.mem.trim(u8, expected, " \n\t");
    const got = std.mem.trim(u8, out_file, " \n\t");
    if (!std.mem.eql(
        u8,
        wanted,
        got,
    )) {
        std.debug.print(
            "Mismatch.\nWanted:\n{s}\n\nGot:\n{s}\n",
            .{ wanted, got },
        );
        std.debug.print("First character: '{}', second: '{}'",.{got[0], got[1]});
        std.os.exit(1);
    }
}

fn test_basic_accounts_and_transfers(
    arena: *std.heap.ArenaAllocator,
    tb_binary: []const u8,
    tmp_dir: []const u8,
) !void {
    const allocator = arena.allocator();
    var expected_accounts =
        \\{
        \\  "id": "1",
        \\  "user_data": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [
        \\    "linked"
        \\  ],
        \\  "debits_pending": "0",
        \\  "debits_posted": "10",
        \\  "credits_pending": "0",
        \\  "credits_posted": "0"
        \\}
        \\{
        \\  "id": "2",
        \\  "user_data": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [],
        \\  "debits_pending": "0",
        \\  "debits_posted": "0",
        \\  "credits_pending": "0",
        \\  "credits_posted": "10"
        \\}
    ;

    var expected_transfer =
        \\{
        \\  "id": "2",
        \\  "debit_account_id": "1",
        \\  "credit_account_id": "2",
        \\  "user_data": "0",
        \\  "pending_id": "0",
        \\  "timeout": "0",
        \\  "ledger": "700",
        \\  "code": "10",
        \\  "flags": [],
        \\  "amount": "10"
        \\}
    ;

    try run_many_with_tb(
        arena,
        &[_][]const []const u8{
            try tb_client_command(
                arena,
                tb_binary,
                "create_accounts id=1 flags=linked code=10 ledger=700, id=2 code=10 ledger=700",
            ),
            try tb_client_command(
                arena,
                tb_binary,
                "create_transfers id=1 debit_account_id=1 credit_account_id=2 amount=10 ledger=700 code=10",
            ),
            try tb_client_command_json_out(
                arena,
                tb_binary,
                "lookup_accounts id=1, id=2",
                try std.fmt.allocPrint(allocator, "{s}/out_accounts", .{tmp_dir}),
            ),
            try tb_client_command_json_out(
                arena,
                tb_binary,
                "lookup_transfers id=1",
                try std.fmt.allocPrint(allocator, "{s}/out_transfer", .{tmp_dir}),
            ),
        },
        ".",
    );

    try fail_on_diff(arena, tmp_dir, expected_accounts, "out_accounts");
    try fail_on_diff(arena, tmp_dir, expected_transfer, "out_transfer");
}

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var t = try TmpDir.init(&arena);
    defer t.deinit();

    const tb_binary = try binary_filename(&arena, &[_][]const u8{"tigerbeetle"});

    try test_basic_accounts_and_transfers(&arena, tb_binary, t.path);
}
