const XDB = @import("XDB");
const std = @import("std");

const DateTime = struct {
    year: u32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
};

fn isLeapYear(year: u32) bool {
    return (@rem(year, 4) == 0 and @rem(year, 100) != 0) or (@rem(year, 400) == 0);
}

fn daysInMonth(month: u8, year: u32) u8 {
    return switch (month) {
        1 => 31,
        2 => if (isLeapYear(year)) 29 else 28,
        3 => 31,
        4 => 30,
        5 => 31,
        6 => 30,
        7 => 31,
        8 => 31,
        9 => 30,
        10 => 31,
        11 => 30,
        12 => 31,
        else => unreachable,
    };
}

fn unixTimestampToUTC(timestamp: u64) DateTime {
    const MILLIS_PER_SEC = 1000;
    const SECS_PER_MIN = 60;
    const SECS_PER_HOUR = SECS_PER_MIN * 60;
    const SECS_PER_DAY = SECS_PER_HOUR * 24;

    const seconds = @divTrunc(timestamp, MILLIS_PER_SEC);

    const hour: u8 = @intCast(@divTrunc(@rem(seconds, SECS_PER_DAY), SECS_PER_HOUR));
    const minute: u8 = @intCast(@divTrunc(@rem(seconds, SECS_PER_HOUR), SECS_PER_MIN));

    var days = @divTrunc(seconds, SECS_PER_DAY);
    var year: u32 = 1970;

    while (true) {
        const days_in_year: u16 = if (isLeapYear(year)) 366 else 365;
        if (days >= days_in_year) {
            days -= days_in_year;
            year += 1;
        } else break;
    }

    var month: u8 = 1;
    while (true) {
        const day_of_month = daysInMonth(month, year);
        if (days >= day_of_month) {
            days -= day_of_month;
            month += 1;
        } else break;
    }

    const day: u8 = @intCast(days + 1);

    return DateTime{
        .year = year,
        .month = month,
        .day = day,
        .hour = hour,
        .minute = minute,
    };
}

var log_mutex = std.Thread.Mutex{};
var log_file: ?std.fs.File = null;

const LogLevel = enum {
    INFO,
    ERROR,
    DEBUG,

    fn toString(self: LogLevel) []const u8 {
        return switch (self) {
            .INFO => "INFO",
            .ERROR => "ERROR",
            .DEBUG => "DEBUG",
        };
    }
};

fn log(level: LogLevel, comptime msg: []const u8, args: anytype) void {
    const timestamp: u64 = @intCast(std.time.milliTimestamp());
    const dt = unixTimestampToUTC(timestamp);
    const stderr = std.io.getStdErr().writer();

    log_mutex.lock();
    defer log_mutex.unlock();

    const log_line = "[{}-{}-{:0>2} {:0>2}:{:0>2}] [{s}] ";
    const log_args = .{ dt.year, dt.month, dt.day, dt.hour, dt.minute, level.toString() };

    stderr.print(log_line, log_args) catch return;
    stderr.print(msg, args) catch return;

    if (log_file) |file| {
        const writer = file.writer();
        writer.print(log_line, log_args) catch {
            stderr.print("[{}-{}-{:0>2} {:0>2}:{:0>2}] [ERROR] Failed to write to log file\n", .{
                dt.year, dt.month, dt.day, dt.hour, dt.minute
            }) catch {};
            return;
        };
        writer.print(msg, args) catch {
            stderr.print("[{}-{}-{:0>2} {:0>2}:{:0>2}] [ERROR] Failed to write to log file\n", .{
                dt.year, dt.month, dt.day, dt.hour, dt.minute
            }) catch {};
            return;
        };
    }
}

fn parseWord(allocator: std.mem.Allocator, val: []const u8) XDB.Word {
    const trimmed_val = std.mem.trim(u8, val, " \t");
    if (std.fmt.parseInt(i64, trimmed_val, 10)) |num| {
        return .{ .int = num };
    } else |_| {
        if (std.fmt.parseFloat(f64, trimmed_val)) |num| {
            return .{ .float = num };
        } else |_| {
            if (std.mem.eql(u8, trimmed_val, "true")) {
                return .{ .boolean = true };
            } else if (std.mem.eql(u8, trimmed_val, "false")) {
                return .{ .boolean = false };
            } else {
                return .{ .string = allocator.dupe(u8, trimmed_val) catch unreachable };
            }
        }
    }
}

fn buildTableJson(allocator: std.mem.Allocator, columns: []const XDB.Column) std.ArrayList(u8) {
    var json = std.ArrayList(u8).init(allocator);
    var writer = json.writer();
    writer.writeByte('[') catch unreachable;
    for (columns, 0..) |col, i| {
        if (i > 0) writer.writeByte(',') catch unreachable;
        writer.writeByte('[') catch unreachable;
        for (col.values.items, 0..) |val, j| {
            if (j > 0) writer.writeByte(',') catch unreachable;
            switch (val) {
                .string => |s| writer.print("\"{}\"", .{std.json.fmt(s, .{})}) catch unreachable,
                .int => |n| writer.print("{}", .{n}) catch unreachable,
                .float => |f| writer.print("{}", .{f}) catch unreachable,
                .boolean => |b| writer.print("{}", .{b}) catch unreachable,
            }
        }
        writer.writeByte(']') catch unreachable;
    }
    writer.writeByte(']') catch unreachable;
    return json;
}

fn buildRowJson(allocator: std.mem.Allocator, row_data: XDB.Row) std.ArrayList(u8) {
    var json = std.ArrayList(u8).init(allocator);
    var writer = json.writer();
    writer.writeByte('[') catch unreachable;
    for (row_data.columns.items, 0..) |col, i| {
        if (i > 0) writer.writeByte(',') catch unreachable;
        writer.writeByte('[') catch unreachable;
        for (col.values.items, 0..) |val, j| {
            if (j > 0) writer.writeByte(',') catch unreachable;
            switch (val) {
                .string => |s| writer.print("\"{}\"", .{std.json.fmt(s, .{})}) catch unreachable,
                .int => |n| writer.print("{}", .{n}) catch unreachable,
                .float => |f| writer.print("{}", .{f}) catch unreachable,
                .boolean => |b| writer.print("{}", .{b}) catch unreachable,
            }
        }
        writer.writeByte(']') catch unreachable;
    }
    writer.writeByte(']') catch unreachable;
    return json;
}

fn buildStringListJson(allocator: std.mem.Allocator, items: []const []const u8) std.ArrayList(u8) {
    var json = std.ArrayList(u8).init(allocator);
    var writer = json.writer();
    writer.writeByte('[') catch unreachable;
    for (items, 0..) |item, i| {
        if (i > 0) writer.writeByte(',') catch unreachable;
        writer.print("\"{}\"", .{std.json.fmt(item, .{})}) catch unreachable;
    }
    writer.writeByte(']') catch unreachable;
    return json;
}

fn handleClient(conn: std.net.Server.Connection) void {
    log(.INFO, "Accepted connection from {}\n", .{conn.address});
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var db = XDB.Database.init();
    db.addUser("root", "1111") catch @panic("could not create root user");
    defer db.deinit();
    var authenticated = false;
    var clientConnected = true;
    var buffer: [1024]u8 = undefined;

    while (clientConnected) {
        const msg = conn.stream.reader().readUntilDelimiterOrEof(&buffer, '\n') catch |err| {
            log(.ERROR, "Read error from {}: {}\n", .{conn.address, err});
            break;
        } orelse break;

        const trimmed = std.mem.trim(u8, msg, " \t\r\n");
        if (trimmed.len == 0) continue;

        if (std.mem.eql(u8, trimmed, "QUIT")) {
            log(.INFO, "Client {} disconnected\n", .{conn.address});
            clientConnected = false;
            continue;
        }

        var tokens = std.mem.splitAny(u8, trimmed, " ");
        const cmd = tokens.next() orelse {
            conn.stream.writer().print("ERROR: missing command\n", .{}) catch {};
            continue;
        };

        if (std.mem.eql(u8, cmd, "LOAD")) {
            const db_path = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing db path\n", .{}) catch {};
                continue;
            };
            XDB.log_level = .NOTHING;
            db = XDB.Database.load(db_path) catch {
                conn.stream.writer().print("ERROR: could not open db\n", .{}) catch {};
                continue;
            };
            XDB.log_level = .FULL;

            conn.stream.writer().print("OK: LOADED DATABASE\n", .{}) catch {};
            authenticated = false;

            log(.INFO, "User at {} loaded database '{s}'\n", .{conn.address, db_path});
            continue;
        }

        if (!authenticated and !std.mem.eql(u8, cmd, "AUTH")) {
            conn.stream.writer().print("ERROR: must authenticate first\n", .{}) catch {};
            continue;
        }
        
        if (std.mem.eql(u8, cmd, "HELP")) {
            const help_text =
                \\HELP: Available Commands
                \\AUTH <user> <password> - Authenticate with username and password
                \\LOAD <db_path> - Load a database from the specified path
                \\CREATE TABLE <table> - Create a new table
                \\ADD ROW <table> <row> - Add a new row to a table
                \\ADD COLUMN <table> <row> values=<value1,value2,...> - Add a column with values to a row
                \\GET TABLE <table> - Retrieve all columns in a table as JSON
                \\GET ROW <table> <row> - Retrieve a specific row in a table as JSON
                \\UPDATE COLUMN <table> <row> <column_index> values=<value1,value2,...> - Update a column in a row
                \\DELETE TABLE <table> - Delete a table
                \\DELETE ROW <table> <row> - Delete a row from a table
                \\DELETE COLUMN <table> <row> <column_index> - Delete a column from a row
                \\LIST TABLES - List all tables as JSON
                \\LIST ROWS <table> - List all rows in a table as JSON
                \\LIST USERS - List all users as JSON
                \\USER ADD <user> <password> - Add a new user
                \\USER REMOVE <user> - Remove a user
                \\USER UPDATE <user> <password> - Update a user's password
                \\SAVE <db_file> - Save the database to a file
                \\QUIT - Disconnect from the server
                \\HELP - Display this help message
                \\
                ;
            conn.stream.writer().print("OK: {s}\n", .{help_text}) catch {};
            log(.INFO, "Help requested by {}\n", .{conn.address});
            continue;
        }

        if (std.mem.eql(u8, cmd, "AUTH")) {
            const user = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing user\n", .{}) catch {};
                continue;
            };
            const password = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing password\n", .{}) catch {};
                continue;
            };
            authenticated = db.verifyUser(user, password) catch |err| {
                conn.stream.writer().print("ERROR: auth failed - {}\n", .{err}) catch {};
                log(.ERROR, "Authentication failed for user {s} from {}: {}\n", .{user, conn.address, err});
                continue;
            };
            conn.stream.writer().print("OK: authenticated\n", .{}) catch {};
            log(.INFO, "User {s} authenticated from {}\n", .{user, conn.address});
            continue;
        }
        
        if (std.mem.eql(u8, cmd, "CREATE")) {
            const subcmd = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing subcommand\n", .{}) catch {};
                continue;
            };
            if (std.mem.eql(u8, subcmd, "TABLE")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                db.createTable(table) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to create table {s} from {}: {}\n", .{table, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: table created\n", .{}) catch {};
                log(.INFO, "Table {s} created by {}\n", .{table, conn.address});
                continue;
            }
            conn.stream.writer().print("ERROR: invalid subcommand\n", .{}) catch {};
            continue;
        }

        if (std.mem.eql(u8, cmd, "ADD")) {
            const subcmd = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing subcommand\n", .{}) catch {};
                continue;
            };
            if (std.mem.eql(u8, subcmd, "ROW")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const row = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing row name\n", .{}) catch {};
                    continue;
                };
                db.addRow(table, row) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to add row {s} to table {s} from {}: {}\n", .{row, table, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: row added\n", .{}) catch {};
                log(.INFO, "Row {s} added to table {s} by {}\n", .{row, table, conn.address});
                continue;
            }
            if (std.mem.eql(u8, subcmd, "COLUMN")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const row = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing row name\n", .{}) catch {};
                    continue;
                };
                const values_str = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing values\n", .{}) catch {};
                    continue;
                };
                if (!std.mem.startsWith(u8, values_str, "values=")) {
                    conn.stream.writer().print("ERROR: invalid values format\n", .{}) catch {};
                    continue;
                }
                const values_list = values_str[7..];
                var word_list = std.ArrayList(XDB.Word).init(allocator);
                defer {
                    for (word_list.items) |word| word.deinit();
                    word_list.deinit();
                }
                var val_tokens = std.mem.splitAny(u8, values_list, ",");
                while (val_tokens.next()) |val| {
                    const word = parseWord(allocator, val);
                    word_list.append(word) catch unreachable;
                }
                db.addColumn(table, row, word_list.items) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to add column to row {s} in table {s} from {}: {}\n", .{row, table, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: column added\n", .{}) catch {};
                log(.INFO, "Column added to row {s} in table {s} by {}\n", .{row, table, conn.address});
                continue;
            }
            conn.stream.writer().print("ERROR: invalid subcommand\n", .{}) catch {};
            continue;
        }

        if (std.mem.eql(u8, cmd, "GET")) {
            const subcmd = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing subcommand\n", .{}) catch {};
                continue;
            };
            if (std.mem.eql(u8, subcmd, "TABLE")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const columns = db.getColumns(table) orelse {
                    conn.stream.writer().print("ERROR: table not found\n", .{}) catch {};
                    log(.ERROR, "Table {s} not found for {}\n", .{table, conn.address});
                    continue;
                };
                const json_result = buildTableJson(allocator, columns);
                defer json_result.deinit();
                conn.stream.writer().print("DATA: table {}\n", .{std.json.fmt(json_result.items, .{})}) catch {};
                log(.INFO, "Table {s} data retrieved by {}\n", .{table, conn.address});
                continue;
            }
            if (std.mem.eql(u8, subcmd, "ROW")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const row = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing row name\n", .{}) catch {};
                    continue;
                };
                const row_data = db.getRow(table, row) orelse {
                    conn.stream.writer().print("ERROR: row not found\n", .{}) catch {};
                    log(.ERROR, "Row {s} in table {s} not found for {}\n", .{row, table, conn.address});
                    continue;
                };
                const json_result = buildRowJson(allocator, row_data);
                defer json_result.deinit();
                conn.stream.writer().print("DATA: row {}\n", .{std.json.fmt(json_result.items, .{})}) catch {};
                log(.INFO, "Row {s} in table {s} retrieved by {}\n", .{row, table, conn.address});
                continue;
            }
            conn.stream.writer().print("ERROR: invalid subcommand\n", .{}) catch {};
            continue;
        }

        if (std.mem.eql(u8, cmd, "UPDATE")) {
            const subcmd = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing subcommand\n", .{}) catch {};
                continue;
            };
            if (std.mem.eql(u8, subcmd, "COLUMN")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const row = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing row name\n", .{}) catch {};
                    continue;
                };
                const col_idx = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing column index\n", .{}) catch {};
                    continue;
                };
                const values_str = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing values\n", .{}) catch {};
                    continue;
                };
                if (!std.mem.startsWith(u8, values_str, "values=")) {
                    conn.stream.writer().print("ERROR: invalid values format\n", .{}) catch {};
                    continue;
                }
                const values_list = values_str[7..];
                const col_num = std.fmt.parseInt(usize, col_idx, 10) catch {
                    conn.stream.writer().print("ERROR: invalid column index\n", .{}) catch {};
                    continue;
                };
                var word_list = std.ArrayList(XDB.Word).init(allocator);
                defer {
                    for (word_list.items) |word| word.deinit();
                    word_list.deinit();
                }
                var val_tokens = std.mem.splitAny(u8, values_list, ",");
                while (val_tokens.next()) |val| {
                    const word = parseWord(allocator, val);
                    word_list.append(word) catch unreachable;
                }
                db.updateColumn(table, row, col_num, word_list.items) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to update column {d} in row {s} of table {s} from {}: {}\n", .{col_num, row, table, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: column updated\n", .{}) catch {};
                log(.INFO, "Column {d} in row {s} of table {s} updated by {}\n", .{col_num, row, table, conn.address});
                continue;
            }
            conn.stream.writer().print("ERROR: invalid subcommand\n", .{}) catch {};
            continue;
        }

        if (std.mem.eql(u8, cmd, "DELETE")) {
            const subcmd = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing subcommand\n", .{}) catch {};
                continue;
            };
            if (std.mem.eql(u8, subcmd, "COLUMN")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const row = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing row name\n", .{}) catch {};
                    continue;
                };
                const col_idx = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing column index\n", .{}) catch {};
                    continue;
                };
                const col_num = std.fmt.parseInt(usize, col_idx, 10) catch {
                    conn.stream.writer().print("ERROR: invalid column index\n", .{}) catch {};
                    continue;
                };
                db.deleteColumn(table, row, col_num) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to delete column {d} in row {s} of table {s} from {}: {}\n", .{col_num, row, table, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: column deleted\n", .{}) catch {};
                log(.INFO, "Column {d} in row {s} of table {s} deleted by {}\n", .{col_num, row, table, conn.address});
                continue;
            }
            if (std.mem.eql(u8, subcmd, "ROW")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const row = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing row name\n", .{}) catch {};
                    continue;
                };
                db.deleteRow(table, row) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to delete row {s} in table {s} from {}: {}\n", .{row, table, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: row deleted\n", .{}) catch {};
                log(.INFO, "Row {s} in table {s} deleted by {}\n", .{row, table, conn.address});
                continue;
            }
            if (std.mem.eql(u8, subcmd, "TABLE")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                db.deleteTable(table) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to delete table {s} from {}: {}\n", .{table, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: table deleted\n", .{}) catch {};
                log(.INFO, "Table {s} deleted by {}\n", .{table, conn.address});
                continue;
            }
            conn.stream.writer().print("ERROR: invalid subcommand\n", .{}) catch {};
            continue;
        }

        if (std.mem.eql(u8, cmd, "LIST")) {
            const subcmd = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing subcommand\n", .{}) catch {};
                continue;
            };
            if (std.mem.eql(u8, subcmd, "TABLES")) {
                const tables = db.listTables();
                const json_result = buildStringListJson(allocator, tables);
                defer json_result.deinit();
                conn.stream.writer().print("DATA: tables {}\n", .{std.json.fmt(json_result.items, .{})}) catch {};
                log(.INFO, "Listed tables for {}\n", .{conn.address});
                continue;
            }
            if (std.mem.eql(u8, subcmd, "ROWS")) {
                const table = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing table name\n", .{}) catch {};
                    continue;
                };
                const rows = db.listRows(table) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to list rows for table {s} from {}: {}\n", .{table, conn.address, err});
                    continue;
                };
                const json_result = buildStringListJson(allocator, rows);
                defer json_result.deinit();
                conn.stream.writer().print("DATA: rows {}\n", .{std.json.fmt(json_result.items, .{})}) catch {};
                log(.INFO, "Listed rows for table {s} by {}\n", .{table, conn.address});
                continue;
            }
            if (std.mem.eql(u8, subcmd, "USERS")) {
                const users = db.listUsers();
                const json_result = buildStringListJson(allocator, users);
                defer json_result.deinit();
                conn.stream.writer().print("DATA: users {}\n", .{std.json.fmt(json_result.items, .{})}) catch {};
                log(.INFO, "Listed users for {}\n", .{conn.address});
                continue;
            }
            conn.stream.writer().print("ERROR: invalid subcommand\n", .{}) catch {};
            continue;
        }

        if (std.mem.eql(u8, cmd, "USER")) {
            const action = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing action\n", .{}) catch {};
                continue;
            };
            if (std.mem.eql(u8, action, "ADD")) {
                const user = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing user\n", .{}) catch {};
                    continue;
                };
                const password = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing password\n", .{}) catch {};
                    continue;
                };
                db.addUser(user, password) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to add user {s} from {}: {}\n", .{user, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: user added\n", .{}) catch {};
                log(.INFO, "User {s} added by {}\n", .{user, conn.address});
                continue;
            }
            if (std.mem.eql(u8, action, "REMOVE")) {
                const user = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing user\n", .{}) catch {};
                    continue;
                };
                db.removeUser(user) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to remove user {s} from {}: {}\n", .{user, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: user removed\n", .{}) catch {};
                log(.INFO, "User {s} removed by {}\n", .{user, conn.address});
                continue;
            }
            if (std.mem.eql(u8, action, "UPDATE")) {
                const user = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing user\n", .{}) catch {};
                    continue;
                };
                const password = tokens.next() orelse {
                    conn.stream.writer().print("ERROR: missing password\n", .{}) catch {};
                    continue;
                };
                db.updateUserPassword(user, password) catch |err| {
                    conn.stream.writer().print("ERROR: {}\n", .{err}) catch {};
                    log(.ERROR, "Failed to update password for user {s} from {}: {}\n", .{user, conn.address, err});
                    continue;
                };
                conn.stream.writer().print("OK: user password updated\n", .{}) catch {};
                log(.INFO, "Password updated for user {s} by {}\n", .{user, conn.address});
                continue;
            }
            conn.stream.writer().print("ERROR: invalid action\n", .{}) catch {};
            continue;
        }
        
        if (std.mem.eql(u8, cmd, "SAVE")) {
            const db_file = tokens.next() orelse {
                conn.stream.writer().print("ERROR: missing db file", .{}) catch {};
                continue;
            };
            db.save(db_file) catch |err| {
                conn.stream.writer().print("ERROR: could not save database: {}", .{err}) catch {};
                log(.ERROR, "Could not save Database by request of {}", .{conn.address});
                continue;
            };
            conn.stream.writer().print("OK: saved Database\n", .{}) catch {};
            continue;
        }

        conn.stream.writer().print("ERROR: unknown command\n", .{}) catch {};
    }
    conn.stream.close();
}

var gallocator = std.heap.page_allocator;

pub fn main() !u8 {
    log_file = std.fs.cwd().createFile("server.log", .{ .truncate = false }) catch |err| {
        const stderr = std.io.getStdErr().writer();
        stderr.print("Failed to open log file: {}\n", .{err}) catch {};
        return 1;
    };
    defer if (log_file) |file| file.close();

    const address = try std.net.Address.parseIp4("0.0.0.0", 8080);
    var server = try address.listen(.{});
    log(.INFO, "Server listening at {}\n", .{address});

    var threads = std.ArrayList(std.Thread).init(gallocator);
    defer {
        for (threads.items) |thread| {
            thread.join();
        }
        threads.deinit();
    }

    while (true) {
        const conn = server.accept() catch continue;
        threads.append(std.Thread.spawn(.{}, handleClient, .{conn}) catch @panic("could not create thread")) catch unreachable;
    }
}

